// Package app é a camada de aplicação (command side do CQRS).
// Orquestra: carregar agregado → decidir → append com expectedVersion →
// publicar no bus. Em conflito de concorrência, recarrega e tenta de novo.
package app

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"eventledger/internal/bus"
	"eventledger/internal/domain"
	"eventledger/internal/eventstore"
)

// maxRetries limita as tentativas sob conflito de optimistic locking.
// Conflitos são esperados e benignos (duas operações na mesma conta ao
// mesmo tempo); acima do limite devolvemos erro para o cliente re-tentar.
const maxRetries = 5

var ErrTooManyConflicts = errors.New("too many concurrent updates, retry later")

type App struct {
	store eventstore.Store
	bus   bus.Bus
}

func New(store eventstore.Store, b bus.Bus) *App {
	return &App{store: store, bus: b}
}

func (a *App) Store() eventstore.Store { return a.store }

// OpenAccount cria a conta e, opcionalmente, faz um depósito inicial.
func (a *App) OpenAccount(ctx context.Context, owner string, initialDeposit int64) (string, error) {
	if initialDeposit < 0 {
		return "", domain.ErrInvalidAmount
	}
	id := domain.NewID()
	opened, err := domain.OpenAccount(id, owner)
	if err != nil {
		return "", err
	}
	events := []domain.Event{opened}

	if initialDeposit > 0 {
		acc, err := domain.Replay(domain.Account{}, events)
		if err != nil {
			return "", err
		}
		dep, err := acc.Deposit(initialDeposit, "initial deposit")
		if err != nil {
			return "", err
		}
		events = append(events, dep)
	}

	if err := a.store.Append(ctx, id, 0, events); err != nil {
		return "", fmt.Errorf("open account: %w", err)
	}
	a.publish(events)
	return id, nil
}

func (a *App) Deposit(ctx context.Context, accountID string, amount int64, description string) error {
	return a.mutate(ctx, accountID, func(acc *domain.Account) (domain.Event, error) {
		return acc.Deposit(amount, description)
	})
}

func (a *App) Withdraw(ctx context.Context, accountID string, amount int64, description string) error {
	return a.mutate(ctx, accountID, func(acc *domain.Account) (domain.Event, error) {
		return acc.Withdraw(amount, description)
	})
}

// StartTransfer valida e debita a origem de forma SÍNCRONA (saldo é checado
// aqui, contra o agregado). O crédito no destino acontece de forma assíncrona
// via saga. Retorna o transfer_id para acompanhamento em GET /transfers/{id}.
func (a *App) StartTransfer(ctx context.Context, from, to string, amount int64) (string, error) {
	transferID := domain.NewID()
	err := a.mutate(ctx, from, func(acc *domain.Account) (domain.Event, error) {
		return acc.DebitTransfer(transferID, to, amount)
	})
	if err != nil {
		return "", err
	}
	return transferID, nil
}

// CreditTransfer é chamado pela saga. Idempotente por transfer_id:
// sob entrega at-least-once, a segunda tentativa vira no-op.
func (a *App) CreditTransfer(ctx context.Context, to, transferID, from string, amount int64) error {
	done, err := a.store.HasTransferEvent(ctx, domain.EventTransferCredited, transferID)
	if err != nil {
		return err
	}
	if done {
		return nil
	}
	return a.mutate(ctx, to, func(acc *domain.Account) (domain.Event, error) {
		return acc.CreditTransfer(transferID, from, amount)
	})
}

// ReverseTransfer compensa o débito de uma transferência que falhou.
func (a *App) ReverseTransfer(ctx context.Context, from, transferID string, amount int64, reason string) error {
	done, err := a.store.HasTransferEvent(ctx, domain.EventTransferReversed, transferID)
	if err != nil {
		return err
	}
	if done {
		return nil
	}
	return a.mutate(ctx, from, func(acc *domain.Account) (domain.Event, error) {
		return acc.ReverseTransfer(transferID, amount, reason)
	})
}

// mutate implementa o loop load → decide → append com retry em conflito.
// Este loop é o que torna impossível saldo negativo sob concorrência:
// se dois writers leram o mesmo estado, só um consegue gravar; o outro
// recarrega o estado JÁ ATUALIZADO e re-valida a regra de saldo.
func (a *App) mutate(ctx context.Context, aggregateID string, decide func(*domain.Account) (domain.Event, error)) error {
	for attempt := 0; attempt < maxRetries; attempt++ {
		acc, err := a.store.Load(ctx, aggregateID)
		if err != nil {
			return err
		}
		event, err := decide(&acc)
		if err != nil {
			return err
		}
		err = a.store.Append(ctx, aggregateID, acc.Version, []domain.Event{event})
		if err == nil {
			a.publish([]domain.Event{event})
			return nil
		}
		if !errors.Is(err, eventstore.ErrConcurrency) {
			return err
		}
		// backoff com jitter para descolar writers em corrida
		select {
		case <-time.After(time.Duration(rand.Intn(20*(attempt+1))) * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return ErrTooManyConflicts
}

func (a *App) publish(events []domain.Event) {
	for _, e := range events {
		a.bus.Publish(e)
	}
}
