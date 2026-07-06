package eventstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/lib/pq"

	"eventledger/internal/domain"
)

// Postgres implementa Store sobre uma tabela append-only.
// O optimistic locking é delegado à constraint UNIQUE(aggregate_id, version):
// não há SELECT FOR UPDATE nem lock de linha — writers concorrentes colidem
// no INSERT e o perdedor recebe ErrConcurrency.
type Postgres struct {
	db *sql.DB
}

func NewPostgres(db *sql.DB) *Postgres { return &Postgres{db: db} }

func (s *Postgres) Append(ctx context.Context, aggregateID string, expectedVersion int64, events []domain.Event) error {
	if len(events) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op após commit

	for i, e := range events {
		if e.AggregateID != aggregateID {
			return ErrAggregateMismatch
		}
		wantVersion := expectedVersion + int64(i) + 1
		if e.Version != wantVersion {
			return fmt.Errorf("event %s has version %d, want %d", e.ID, e.Version, wantVersion)
		}
		_, err := tx.ExecContext(ctx,
			`INSERT INTO events (event_id, aggregate_id, version, type, data, occurred_at)
			 VALUES ($1, $2, $3, $4, $5, $6)`,
			e.ID, e.AggregateID, e.Version, string(e.Type), []byte(e.Data), e.OccurredAt,
		)
		if err != nil {
			var pqErr *pq.Error
			if errors.As(err, &pqErr) && pqErr.Code == "23505" { // unique_violation
				return ErrConcurrency
			}
			return fmt.Errorf("insert event: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	// Snapshot fora da transação de escrita: é otimização, não correção.
	// Se falhar, o Load reconstrói pelo stream do mesmo jeito.
	last := events[len(events)-1]
	if last.Version%SnapshotEvery == 0 {
		s.saveSnapshot(ctx, aggregateID)
	}
	return nil
}

func (s *Postgres) Load(ctx context.Context, aggregateID string) (domain.Account, error) {
	var base domain.Account
	var state []byte
	var snapVersion int64
	err := s.db.QueryRowContext(ctx,
		`SELECT version, state FROM snapshots WHERE aggregate_id = $1`, aggregateID,
	).Scan(&snapVersion, &state)
	switch {
	case err == nil:
		if err := json.Unmarshal(state, &base); err != nil {
			return domain.Account{}, fmt.Errorf("unmarshal snapshot: %w", err)
		}
	case errors.Is(err, sql.ErrNoRows):
		// sem snapshot: replay completo
	default:
		return domain.Account{}, fmt.Errorf("load snapshot: %w", err)
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT seq, event_id, aggregate_id, version, type, data, occurred_at
		   FROM events
		  WHERE aggregate_id = $1 AND version > $2
		  ORDER BY version ASC`,
		aggregateID, base.Version,
	)
	if err != nil {
		return domain.Account{}, fmt.Errorf("load events: %w", err)
	}
	defer rows.Close()

	events, err := scanEvents(rows)
	if err != nil {
		return domain.Account{}, err
	}
	if base.Version == 0 && len(events) == 0 {
		return domain.Account{}, domain.ErrAccountNotFound
	}
	return domain.Replay(base, events)
}

func (s *Postgres) saveSnapshot(ctx context.Context, aggregateID string) {
	acc, err := s.Load(ctx, aggregateID)
	if err != nil {
		return // best-effort
	}
	state, err := json.Marshal(acc)
	if err != nil {
		return
	}
	_, _ = s.db.ExecContext(ctx,
		`INSERT INTO snapshots (aggregate_id, version, state, updated_at)
		 VALUES ($1, $2, $3, now())
		 ON CONFLICT (aggregate_id)
		 DO UPDATE SET version = EXCLUDED.version, state = EXCLUDED.state, updated_at = now()
		 WHERE snapshots.version < EXCLUDED.version`,
		aggregateID, acc.Version, state,
	)
}

func (s *Postgres) LoadUnprocessed(ctx context.Context) ([]domain.Event, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT e.seq, e.event_id, e.aggregate_id, e.version, e.type, e.data, e.occurred_at
		   FROM events e
		   LEFT JOIN processed_events p ON p.event_id = e.event_id
		  WHERE p.event_id IS NULL
		  ORDER BY e.seq ASC`)
	if err != nil {
		return nil, fmt.Errorf("load unprocessed: %w", err)
	}
	defer rows.Close()
	return scanEvents(rows)
}

func (s *Postgres) PendingTransferDebits(ctx context.Context) ([]domain.Event, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT e.seq, e.event_id, e.aggregate_id, e.version, e.type, e.data, e.occurred_at
		   FROM events e
		  WHERE e.type = $1
		    AND NOT EXISTS (
		        SELECT 1 FROM events c
		         WHERE c.type IN ($2, $3)
		           AND c.data ->> 'transfer_id' = e.data ->> 'transfer_id')
		  ORDER BY e.seq ASC`,
		string(domain.EventTransferDebited),
		string(domain.EventTransferCredited),
		string(domain.EventTransferReversed),
	)
	if err != nil {
		return nil, fmt.Errorf("pending transfer debits: %w", err)
	}
	defer rows.Close()
	return scanEvents(rows)
}

func (s *Postgres) HasTransferEvent(ctx context.Context, typ domain.EventType, transferID string) (bool, error) {
	var exists bool
	err := s.db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM events WHERE type = $1 AND data ->> 'transfer_id' = $2)`,
		string(typ), transferID,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("has transfer event: %w", err)
	}
	return exists, nil
}

func (s *Postgres) EventsByAggregate(ctx context.Context, aggregateID string) ([]domain.Event, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT seq, event_id, aggregate_id, version, type, data, occurred_at
		   FROM events WHERE aggregate_id = $1 ORDER BY version ASC`, aggregateID)
	if err != nil {
		return nil, fmt.Errorf("events by aggregate: %w", err)
	}
	defer rows.Close()
	return scanEvents(rows)
}

func scanEvents(rows *sql.Rows) ([]domain.Event, error) {
	var out []domain.Event
	for rows.Next() {
		var e domain.Event
		var typ string
		var data []byte
		if err := rows.Scan(&e.Seq, &e.ID, &e.AggregateID, &e.Version, &typ, &data, &e.OccurredAt); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		e.Type = domain.EventType(typ)
		e.Data = json.RawMessage(data)
		out = append(out, e)
	}
	return out, rows.Err()
}
