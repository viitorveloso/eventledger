// Package projection materializa os read models (query side do CQRS).
//
// Idempotência: cada evento é aplicado dentro de uma transação que começa
// inserindo em processed_events (PK = event_id). Se o INSERT não afeta
// linha, o evento é duplicata e a transação vira no-op. Aplicação do efeito
// e marca de processado são atômicas — não existe "aplicou mas não marcou".
//
// O projetor roda como handler síncrono do bus: como não publica eventos,
// não há risco de deadlock, e a ordem por conta (garantida pela partição)
// é preservada sem esforço.
package projection

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"

	"eventledger/internal/domain"
)

type Projector struct {
	db  *sql.DB
	log *slog.Logger
}

func New(db *sql.DB, log *slog.Logger) *Projector {
	return &Projector{db: db, log: log}
}

// Handle é o subscriber registrado no bus.
func (p *Projector) Handle(ctx context.Context, e domain.Event) {
	if err := p.Apply(ctx, e); err != nil {
		// O evento continua fora de processed_events, então o catch-up do
		// próximo boot reprocessa. Trilha de escrita não é afetada.
		p.log.Error("projection failed", "event_id", e.ID, "type", e.Type, "err", err)
	}
}

// CatchUp aplica todos os eventos ainda não projetados, em ordem de seq.
// Roda no boot: cobre tanto o gap "gravou no store mas caiu antes de
// publicar no bus" quanto falhas anteriores de projeção. É, na prática,
// um relay de Transactional Outbox — a tabela events É o outbox.
func (p *Projector) CatchUp(ctx context.Context, events []domain.Event) error {
	for _, e := range events {
		if err := p.Apply(ctx, e); err != nil {
			return fmt.Errorf("catch-up event %s: %w", e.ID, err)
		}
	}
	return nil
}

func (p *Projector) Apply(ctx context.Context, e domain.Event) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	res, err := tx.ExecContext(ctx,
		`INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`, e.ID)
	if err != nil {
		return fmt.Errorf("mark processed: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return tx.Commit() // duplicata (at-least-once): no-op
	}

	if err := p.applyEvent(ctx, tx, e); err != nil {
		return err
	}
	return tx.Commit()
}

func (p *Projector) applyEvent(ctx context.Context, tx *sql.Tx, e domain.Event) error {
	switch e.Type {
	case domain.EventAccountOpened:
		var d domain.AccountOpened
		if err := json.Unmarshal(e.Data, &d); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx,
			`INSERT INTO account_balances (account_id, owner, balance, open, last_version)
			 VALUES ($1, $2, 0, TRUE, $3)
			 ON CONFLICT (account_id) DO NOTHING`,
			e.AggregateID, d.Owner, e.Version)
		return err

	case domain.EventMoneyDeposited:
		var d domain.MoneyDeposited
		if err := json.Unmarshal(e.Data, &d); err != nil {
			return err
		}
		if err := p.bump(ctx, tx, e, +d.Amount); err != nil {
			return err
		}
		return p.statement(ctx, tx, e, "deposit", +d.Amount, "", "", d.Description)

	case domain.EventMoneyWithdrawn:
		var d domain.MoneyWithdrawn
		if err := json.Unmarshal(e.Data, &d); err != nil {
			return err
		}
		if err := p.bump(ctx, tx, e, -d.Amount); err != nil {
			return err
		}
		return p.statement(ctx, tx, e, "withdrawal", -d.Amount, "", "", d.Description)

	case domain.EventTransferDebited:
		var d domain.TransferDebited
		if err := json.Unmarshal(e.Data, &d); err != nil {
			return err
		}
		if err := p.bump(ctx, tx, e, -d.Amount); err != nil {
			return err
		}
		if err := p.statement(ctx, tx, e, "transfer_out", -d.Amount, d.To, d.TransferID, ""); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx,
			`INSERT INTO transfers (transfer_id, from_id, to_id, amount, status, created_at, updated_at)
			 VALUES ($1, $2, $3, $4, 'pending', $5, $5)
			 ON CONFLICT (transfer_id) DO NOTHING`,
			d.TransferID, e.AggregateID, d.To, d.Amount, e.OccurredAt)
		return err

	case domain.EventTransferCredited:
		var d domain.TransferCredited
		if err := json.Unmarshal(e.Data, &d); err != nil {
			return err
		}
		if err := p.bump(ctx, tx, e, +d.Amount); err != nil {
			return err
		}
		if err := p.statement(ctx, tx, e, "transfer_in", +d.Amount, d.From, d.TransferID, ""); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx,
			`UPDATE transfers SET status = 'completed', updated_at = $2 WHERE transfer_id = $1`,
			d.TransferID, e.OccurredAt)
		return err

	case domain.EventTransferReversed:
		var d domain.TransferReversed
		if err := json.Unmarshal(e.Data, &d); err != nil {
			return err
		}
		if err := p.bump(ctx, tx, e, +d.Amount); err != nil {
			return err
		}
		if err := p.statement(ctx, tx, e, "transfer_reversal", +d.Amount, "", d.TransferID, d.Reason); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx,
			`UPDATE transfers SET status = 'reversed', reason = $2, updated_at = $3 WHERE transfer_id = $1`,
			d.TransferID, d.Reason, e.OccurredAt)
		return err
	}
	return nil // tipo desconhecido: ignora (forward compatibility)
}

func (p *Projector) bump(ctx context.Context, tx *sql.Tx, e domain.Event, delta int64) error {
	res, err := tx.ExecContext(ctx,
		`UPDATE account_balances
		    SET balance = balance + $2, last_version = $3, updated_at = $4
		  WHERE account_id = $1`,
		e.AggregateID, delta, e.Version, e.OccurredAt)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("projection invariant failed: account %s missing for event %s", e.AggregateID, e.ID)
	}
	return nil
}

func (p *Projector) statement(ctx context.Context, tx *sql.Tx, e domain.Event,
	kind string, amount int64, counterparty, transferID, description string) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO statement_entries
		     (event_id, account_id, kind, amount, counterparty, transfer_id, description, occurred_at)
		 VALUES ($1, $2, $3, $4, NULLIF($5,'')::uuid, NULLIF($6,'')::uuid, $7, $8)
		 ON CONFLICT (event_id) DO NOTHING`,
		e.ID, e.AggregateID, kind, amount, counterparty, transferID, description, e.OccurredAt)
	return err
}
