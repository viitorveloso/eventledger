// Package query serve as leituras (query side do CQRS).
// Consulta APENAS os read models materializados — o event store nunca é
// tocado para atender GET /balance ou GET /statement. Em produção, este
// pacote apontaria para Redis (saldo) e réplica de leitura (extrato);
// a API não saberia a diferença.
package query

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"eventledger/internal/domain"
)

type Balance struct {
	AccountID string    `json:"account_id"`
	Owner     string    `json:"owner"`
	Balance   int64     `json:"balance_cents"`
	Open      bool      `json:"open"`
	UpdatedAt time.Time `json:"updated_at"`
}

type StatementEntry struct {
	EventID      string    `json:"event_id"`
	Kind         string    `json:"kind"`
	Amount       int64     `json:"amount_cents"`
	Counterparty string    `json:"counterparty,omitempty"`
	TransferID   string    `json:"transfer_id,omitempty"`
	Description  string    `json:"description,omitempty"`
	OccurredAt   time.Time `json:"occurred_at"`
}

type Transfer struct {
	TransferID string    `json:"transfer_id"`
	FromID     string    `json:"from_account_id"`
	ToID       string    `json:"to_account_id"`
	Amount     int64     `json:"amount_cents"`
	Status     string    `json:"status"`
	Reason     string    `json:"reason,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type Queries struct {
	db *sql.DB
}

func New(db *sql.DB) *Queries { return &Queries{db: db} }

func (q *Queries) Ping(ctx context.Context) error {
	if q.db == nil {
		return errors.New("query database is not configured")
	}
	return q.db.PingContext(ctx)
}

func (q *Queries) Balance(ctx context.Context, accountID string) (Balance, error) {
	var b Balance
	err := q.db.QueryRowContext(ctx,
		`SELECT account_id, owner, balance, open, updated_at
		   FROM account_balances WHERE account_id = $1`, accountID,
	).Scan(&b.AccountID, &b.Owner, &b.Balance, &b.Open, &b.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Balance{}, domain.ErrAccountNotFound
	}
	return b, err
}

// Statement pagina por cursor (occurred_at do último item da página
// anterior) — paginação estável mesmo com inserções concorrentes,
// diferente de OFFSET.
func (q *Queries) Statement(ctx context.Context, accountID string, limit int, before time.Time) ([]StatementEntry, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	rows, err := q.db.QueryContext(ctx,
		`SELECT event_id, kind, amount,
		        COALESCE(counterparty::text, ''), COALESCE(transfer_id::text, ''),
		        description, occurred_at
		   FROM statement_entries
		  WHERE account_id = $1 AND occurred_at < $2
		  ORDER BY occurred_at DESC, event_id DESC
		  LIMIT $3`,
		accountID, before, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []StatementEntry{}
	for rows.Next() {
		var s StatementEntry
		if err := rows.Scan(&s.EventID, &s.Kind, &s.Amount,
			&s.Counterparty, &s.TransferID, &s.Description, &s.OccurredAt); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func (q *Queries) Transfer(ctx context.Context, transferID string) (Transfer, error) {
	var t Transfer
	err := q.db.QueryRowContext(ctx,
		`SELECT transfer_id, from_id, to_id, amount, status, reason, created_at, updated_at
		   FROM transfers WHERE transfer_id = $1`, transferID,
	).Scan(&t.TransferID, &t.FromID, &t.ToID, &t.Amount, &t.Status, &t.Reason, &t.CreatedAt, &t.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Transfer{}, sql.ErrNoRows
	}
	return t, err
}
