// Package e2e liga o sistema inteiro contra um Postgres real:
// comando HTTP-less (app) → event store → bus → saga + projetor → read models.
// É o mesmo wiring do main.go. Roda apenas com TEST_DATABASE_URL definido.
package e2e

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"eventledger/internal/app"
	"eventledger/internal/bus"
	"eventledger/internal/domain"
	"eventledger/internal/eventstore"
	"eventledger/internal/projection"
	"eventledger/internal/query"
	"eventledger/internal/saga"
)

type system struct {
	app     *app.App
	queries *query.Queries
	stop    func()
}

func boot(t *testing.T) *system {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping e2e test")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	schema, err := os.ReadFile(filepath.Join("..", "..", "migrations", "001_schema.sql"))
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	if _, err := db.Exec(string(schema)); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	if _, err := db.Exec(`TRUNCATE events, snapshots, idempotency_keys, account_balances,
		statement_entries, transfers, processed_events`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	store := eventstore.NewPostgres(db)
	b := bus.NewInMemory(4, 256)
	a := app.New(store, b)
	proj := projection.New(db, log)
	sg := saga.New(a, store, log)

	b.Subscribe(proj.Handle)
	b.Subscribe(sg.Handle)
	ctx, cancel := context.WithCancel(context.Background())
	b.Start(ctx)
	sg.Start(ctx, 2)

	return &system{
		app:     a,
		queries: query.New(db),
		stop: func() {
			b.Close()
			sg.Close()
			cancel()
			db.Close()
		},
	}
}

func (s *system) waitTransferStatus(t *testing.T, tid, want string) query.Transfer {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last query.Transfer
	for time.Now().Before(deadline) {
		tr, err := s.queries.Transfer(context.Background(), tid)
		if err == nil {
			last = tr
			if tr.Status == want {
				return tr
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting transfer %s to reach %q; last = %+v", tid, want, last)
	return last
}

func (s *system) waitBalance(t *testing.T, accID string, want int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var got int64 = -1
	for time.Now().Before(deadline) {
		b, err := s.queries.Balance(context.Background(), accID)
		if err == nil {
			got = b.Balance
			if got == want {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout: read model balance = %d, want %d", got, want)
}

func TestEndToEndTransferLifecycle(t *testing.T) {
	s := boot(t)
	defer s.stop()
	ctx := context.Background()

	alice, err := s.app.OpenAccount(ctx, "alice", 200_00)
	if err != nil {
		t.Fatalf("open alice: %v", err)
	}
	bob, err := s.app.OpenAccount(ctx, "bob", 0)
	if err != nil {
		t.Fatalf("open bob: %v", err)
	}

	// Transferência feliz: pending → completed, saldos nos read models.
	tid, err := s.app.StartTransfer(ctx, alice, bob, 80_00)
	if err != nil {
		t.Fatalf("transfer: %v", err)
	}
	s.waitTransferStatus(t, tid, "completed")
	s.waitBalance(t, alice, 120_00)
	s.waitBalance(t, bob, 80_00)

	// Transferência para conta fantasma: pending → reversed, dinheiro volta.
	ghostTid, err := s.app.StartTransfer(ctx, alice, domain.NewID(), 30_00)
	if err != nil {
		t.Fatalf("ghost transfer: %v", err)
	}
	tr := s.waitTransferStatus(t, ghostTid, "reversed")
	if tr.Reason == "" {
		t.Fatal("reversed transfer must carry a reason")
	}
	s.waitBalance(t, alice, 120_00) // 120 - 30 + 30

	// Extrato conta a história completa da alice:
	// deposit(+200), transfer_out(-80), transfer_out(-30), reversal(+30).
	entries, err := s.queries.Statement(ctx, alice, 50, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("statement: %v", err)
	}
	if len(entries) != 4 {
		t.Fatalf("statement entries = %d, want 4: %+v", len(entries), entries)
	}
	var sum int64
	for _, e := range entries {
		sum += e.Amount
	}
	if sum != 120_00 {
		t.Fatalf("statement sums to %d, want 12000", sum)
	}
}

func TestEndToEndConcurrentWithdrawalsOnPostgres(t *testing.T) {
	s := boot(t)
	defer s.stop()
	ctx := context.Background()

	acc, err := s.app.OpenAccount(ctx, "carol", 50_00)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// 20 saques concorrentes de R$10 contra saldo de R$50: no máximo 5 passam.
	done := make(chan error, 20)
	for i := 0; i < 20; i++ {
		go func() {
			done <- s.app.Withdraw(ctx, acc, 10_00, "e2e race")
		}()
	}
	var ok int
	for i := 0; i < 20; i++ {
		if err := <-done; err == nil {
			ok++
		}
	}
	if ok == 0 || ok > 5 {
		t.Fatalf("successful withdrawals = %d, want 1..5", ok)
	}

	s.waitBalance(t, acc, int64(50_00-ok*10_00))
}
