// Package saga implementa a orquestração da transferência entre contas.
//
// Fluxo: transfer.debited → tenta creditar destino → transfer.credited.
// Se o crédito for impossível (conta inexistente/fechada), emite
// transfer.reversed na origem — a compensação que devolve o dinheiro.
//
// A saga NÃO processa dentro do handler do bus: enfileira na própria fila
// e trabalha em workers separados. Isso evita bloquear as partições do bus
// e espelha como um consumer group Kafka dedicado operaria.
package saga

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"eventledger/internal/domain"
	"eventledger/internal/eventstore"
)

// creditRetries: tentativas para erros transitórios (banco fora etc.)
// antes de desistir e compensar.
const creditRetries = 3

type Commands interface {
	CreditTransfer(ctx context.Context, to, transferID, from string, amount int64) error
	ReverseTransfer(ctx context.Context, from, transferID string, amount int64, reason string) error
}

type TransferSaga struct {
	cmd   Commands
	store eventstore.Store
	queue chan domain.Event
	wg    sync.WaitGroup
	log   *slog.Logger
}

func New(cmd Commands, store eventstore.Store, log *slog.Logger) *TransferSaga {
	return &TransferSaga{
		cmd:   cmd,
		store: store,
		queue: make(chan domain.Event, 1024),
		log:   log,
	}
}

// Handle é o subscriber registrado no bus: só filtra e enfileira (rápido).
func (s *TransferSaga) Handle(ctx context.Context, e domain.Event) {
	if e.Type != domain.EventTransferDebited {
		return
	}
	select {
	case s.queue <- e:
	case <-ctx.Done():
	}
}

func (s *TransferSaga) Start(ctx context.Context, workers int) {
	for i := 0; i < workers; i++ {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			for e := range s.queue {
				s.process(ctx, e)
			}
		}()
	}
}

// CatchUp reprocessa débitos pendentes (sem crédito nem estorno) deixados
// por um crash anterior. Chamado no boot, antes de abrir a API.
func (s *TransferSaga) CatchUp(ctx context.Context) error {
	pending, err := s.store.PendingTransferDebits(ctx)
	if err != nil {
		return err
	}
	for _, e := range pending {
		s.log.Info("saga catch-up: resuming transfer", "event_id", e.ID)
		s.queue <- e
	}
	return nil
}

func (s *TransferSaga) Close() {
	close(s.queue)
	s.wg.Wait()
}

func (s *TransferSaga) process(ctx context.Context, e domain.Event) {
	var p domain.TransferDebited
	if err := json.Unmarshal(e.Data, &p); err != nil {
		s.log.Error("saga: bad payload", "event_id", e.ID, "err", err)
		return
	}
	from := e.AggregateID

	var lastErr error
	for attempt := 0; attempt < creditRetries; attempt++ {
		lastErr = s.cmd.CreditTransfer(ctx, p.To, p.TransferID, from, p.Amount)
		if lastErr == nil {
			s.log.Info("saga: transfer completed", "transfer_id", p.TransferID)
			return
		}
		// Erros de negócio não vão se resolver com retry: compensa já.
		if isBusinessError(lastErr) {
			break
		}
		select { // transitório: backoff e tenta de novo
		case <-time.After(time.Duration(100*(attempt+1)) * time.Millisecond):
		case <-ctx.Done():
			return // catch-up do próximo boot retoma esta transferência
		}
	}

	s.log.Warn("saga: credit failed, compensating",
		"transfer_id", p.TransferID, "reason", lastErr.Error())
	if err := s.cmd.ReverseTransfer(ctx, from, p.TransferID, p.Amount, lastErr.Error()); err != nil {
		// Estorno também falhou (ex.: banco fora). O débito continua pendente
		// no event store e o catch-up do próximo boot vai retomar a saga —
		// nenhum dinheiro se perde, no pior caso fica atrasado.
		s.log.Error("saga: compensation failed, will retry on next boot",
			"transfer_id", p.TransferID, "err", err)
	}
}

func isBusinessError(err error) bool {
	return errors.Is(err, domain.ErrAccountNotFound) ||
		errors.Is(err, domain.ErrAccountClosed) ||
		errors.Is(err, domain.ErrInvalidAmount) ||
		errors.Is(err, domain.ErrAmountTooLarge)
}
