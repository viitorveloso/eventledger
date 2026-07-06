// Package eventstore define a porta de persistência de eventos e suas
// implementações (Postgres para produção, memória para testes).
package eventstore

import (
	"context"
	"errors"

	"eventledger/internal/domain"
)

// ErrConcurrency indica colisão de optimistic locking: outro writer gravou
// no mesmo agregado entre o load e o append. O chamador deve recarregar
// o agregado, re-decidir e tentar de novo.
var ErrConcurrency = errors.New("concurrency conflict: aggregate was modified")

var ErrAggregateMismatch = errors.New("event aggregate does not match stream")

// SnapshotEvery define de quantos em quantos eventos um snapshot é salvo.
const SnapshotEvery = 50

type Store interface {
	// Append grava eventos de forma atômica exigindo que a versão atual do
	// stream seja expectedVersion. Retorna ErrConcurrency em caso de corrida.
	Append(ctx context.Context, aggregateID string, expectedVersion int64, events []domain.Event) error

	// Load reconstrói o agregado: snapshot (se houver) + eventos posteriores.
	Load(ctx context.Context, aggregateID string) (domain.Account, error)

	// LoadUnprocessed retorna, em ordem de seq, eventos ainda não marcados em
	// processed_events — usado pelo catch-up do projetor no boot (garantia
	// at-least-once mesmo com crash entre gravar e publicar no bus).
	LoadUnprocessed(ctx context.Context) ([]domain.Event, error)

	// PendingTransferDebits retorna débitos de transferência sem crédito nem
	// estorno correspondente — usado pelo catch-up da saga no boot.
	PendingTransferDebits(ctx context.Context) ([]domain.Event, error)

	// HasTransferEvent responde se já existe evento daquele tipo para o
	// transfer_id — dedup por chave de negócio, protege a saga de duplicatas.
	HasTransferEvent(ctx context.Context, typ domain.EventType, transferID string) (bool, error)

	// EventsByAggregate expõe o stream bruto (endpoint de auditoria).
	EventsByAggregate(ctx context.Context, aggregateID string) ([]domain.Event, error)
}
