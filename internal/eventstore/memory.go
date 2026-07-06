package eventstore

import (
	"context"
	"encoding/json"
	"sync"

	"eventledger/internal/domain"
)

// Memory implementa Store em memória com a MESMA semântica de optimistic
// locking do Postgres. Usado nos testes de unidade e de concorrência —
// permite rodar `go test -race` sem infraestrutura.
type Memory struct {
	mu      sync.Mutex
	streams map[string][]domain.Event // por aggregate_id, ordenado por version
	log     []domain.Event            // ordem global (seq)
	seq     int64
	marks   map[string]bool // processed_events simulado (via MarkProcessed)
}

func NewMemory() *Memory {
	return &Memory{
		streams: make(map[string][]domain.Event),
		marks:   make(map[string]bool),
	}
}

func (s *Memory) Append(_ context.Context, aggregateID string, expectedVersion int64, events []domain.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	stream := s.streams[aggregateID]
	current := int64(len(stream))
	if current != expectedVersion {
		return ErrConcurrency
	}
	for i, e := range events {
		if e.AggregateID != aggregateID {
			return ErrAggregateMismatch
		}
		if e.Version != expectedVersion+int64(i)+1 {
			return ErrConcurrency
		}
		s.seq++
		e.Seq = s.seq
		stream = append(stream, e)
		s.log = append(s.log, e)
	}
	s.streams[aggregateID] = stream
	return nil
}

func (s *Memory) Load(_ context.Context, aggregateID string) (domain.Account, error) {
	s.mu.Lock()
	stream := append([]domain.Event(nil), s.streams[aggregateID]...)
	s.mu.Unlock()

	if len(stream) == 0 {
		return domain.Account{}, domain.ErrAccountNotFound
	}
	return domain.Replay(domain.Account{}, stream)
}

func (s *Memory) LoadUnprocessed(_ context.Context) ([]domain.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.Event
	for _, e := range s.log {
		if !s.marks[e.ID] {
			out = append(out, e)
		}
	}
	return out, nil
}

// MarkProcessed simula o INSERT em processed_events feito pelo projetor real.
func (s *Memory) MarkProcessed(id string) {
	s.mu.Lock()
	s.marks[id] = true
	s.mu.Unlock()
}

func (s *Memory) PendingTransferDebits(_ context.Context) ([]domain.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	settled := make(map[string]bool)
	for _, e := range s.log {
		if e.Type == domain.EventTransferCredited || e.Type == domain.EventTransferReversed {
			settled[transferIDOf(e)] = true
		}
	}
	var out []domain.Event
	for _, e := range s.log {
		if e.Type == domain.EventTransferDebited && !settled[transferIDOf(e)] {
			out = append(out, e)
		}
	}
	return out, nil
}

func (s *Memory) HasTransferEvent(_ context.Context, typ domain.EventType, transferID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.log {
		if e.Type == typ && transferIDOf(e) == transferID {
			return true, nil
		}
	}
	return false, nil
}

func (s *Memory) EventsByAggregate(_ context.Context, aggregateID string) ([]domain.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]domain.Event(nil), s.streams[aggregateID]...), nil
}

func transferIDOf(e domain.Event) string {
	var p struct {
		TransferID string `json:"transfer_id"`
	}
	_ = json.Unmarshal(e.Data, &p)
	return p.TransferID
}
