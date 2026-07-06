package eventstore

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	_ "github.com/lib/pq"

	"eventledger/internal/domain"
)

// setupPG conecta no banco de teste, aplica o schema e limpa as tabelas.
// Sem TEST_DATABASE_URL o teste é pulado — CI define a variável e roda
// contra um service container.
func setupPG(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping Postgres integration test")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	schema, err := os.ReadFile(filepath.Join("..", "..", "migrations", "001_schema.sql"))
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	if _, err := db.Exec(string(schema)); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	_, err = db.Exec(`TRUNCATE events, snapshots, idempotency_keys, account_balances,
		statement_entries, transfers, processed_events`)
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return db
}

func TestAppendLoadRoundTrip(t *testing.T) {
	db := setupPG(t)
	store := NewPostgres(db)
	ctx := context.Background()

	id := domain.NewID()
	opened, _ := domain.OpenAccount(id, "lucas")
	if err := store.Append(ctx, id, 0, []domain.Event{opened}); err != nil {
		t.Fatalf("append: %v", err)
	}

	acc, err := store.Load(ctx, id)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	dep, err := acc.Deposit(77_00, "")
	if err != nil {
		t.Fatalf("deposit: %v", err)
	}
	if err := store.Append(ctx, id, acc.Version, []domain.Event{dep}); err != nil {
		t.Fatalf("append deposit: %v", err)
	}

	acc, err = store.Load(ctx, id)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if acc.Balance != 77_00 || acc.Version != 2 || acc.Owner != "lucas" {
		t.Fatalf("state = %+v", acc)
	}
}

// A constraint UNIQUE(aggregate_id, version) em ação: N writers preparam a
// escrita sobre a MESMA versão do agregado (barreira antes do append) e
// tentam gravar ao mesmo tempo — o banco deixa exatamente um passar.
func TestOptimisticLockingUnderRealConcurrency(t *testing.T) {
	db := setupPG(t)
	store := NewPostgres(db)
	ctx := context.Background()

	id := domain.NewID()
	opened, _ := domain.OpenAccount(id, "lucas")
	if err := store.Append(ctx, id, 0, []domain.Event{opened}); err != nil {
		t.Fatalf("append: %v", err)
	}

	const writers = 20

	// Fase 1: todos carregam o MESMO estado (version=1) e decidem.
	type attempt struct {
		expected int64
		ev       domain.Event
	}
	prepared := make([]attempt, writers)
	for i := range prepared {
		acc, err := store.Load(ctx, id)
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		ev, err := acc.Deposit(1_00, "race")
		if err != nil {
			t.Fatalf("decide: %v", err)
		}
		prepared[i] = attempt{expected: acc.Version, ev: ev}
	}

	// Fase 2: barreira — todos os appends disparam juntos.
	start := make(chan struct{})
	var wins, conflicts atomic.Int64
	var wg sync.WaitGroup
	for _, a := range prepared {
		wg.Add(1)
		go func(a attempt) {
			defer wg.Done()
			<-start
			err := store.Append(ctx, id, a.expected, []domain.Event{a.ev})
			switch {
			case err == nil:
				wins.Add(1)
			case errors.Is(err, ErrConcurrency):
				conflicts.Add(1)
			default:
				t.Errorf("unexpected: %v", err)
			}
		}(a)
	}
	close(start)
	wg.Wait()

	if wins.Load() != 1 {
		t.Fatalf("exactly one writer must win, got %d (conflicts=%d)", wins.Load(), conflicts.Load())
	}
	if conflicts.Load() != writers-1 {
		t.Fatalf("conflicts = %d, want %d", conflicts.Load(), writers-1)
	}

	// E o estado final reflete exatamente um depósito.
	acc, err := store.Load(ctx, id)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if acc.Balance != 1_00 || acc.Version != 2 {
		t.Fatalf("state = balance %d version %d, want 100/2", acc.Balance, acc.Version)
	}
}

func TestSnapshotSpeedsUpLoad(t *testing.T) {
	db := setupPG(t)
	store := NewPostgres(db)
	ctx := context.Background()

	id := domain.NewID()
	opened, _ := domain.OpenAccount(id, "lucas")
	if err := store.Append(ctx, id, 0, []domain.Event{opened}); err != nil {
		t.Fatalf("append: %v", err)
	}

	// Gera eventos suficientes para cruzar o limiar de snapshot.
	for i := 0; i < SnapshotEvery+10; i++ {
		acc, err := store.Load(ctx, id)
		if err != nil {
			t.Fatalf("load #%d: %v", i, err)
		}
		ev, _ := acc.Deposit(1_00, "")
		if err := store.Append(ctx, id, acc.Version, []domain.Event{ev}); err != nil {
			t.Fatalf("append #%d: %v", i, err)
		}
	}

	var snapVersion int64
	err := db.QueryRow(`SELECT version FROM snapshots WHERE aggregate_id = $1`, id).Scan(&snapVersion)
	if err != nil {
		t.Fatalf("snapshot missing: %v", err)
	}
	if snapVersion < SnapshotEvery {
		t.Fatalf("snapshot version = %d, want >= %d", snapVersion, SnapshotEvery)
	}

	// Load pós-snapshot continua correto (snapshot + delta).
	acc, err := store.Load(ctx, id)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	wantBalance := int64((SnapshotEvery + 10) * 1_00)
	if acc.Balance != wantBalance {
		t.Fatalf("balance = %d, want %d", acc.Balance, wantBalance)
	}
}

func TestTransferQueries(t *testing.T) {
	db := setupPG(t)
	store := NewPostgres(db)
	ctx := context.Background()

	from := domain.NewID()
	opened, _ := domain.OpenAccount(from, "from")
	if err := store.Append(ctx, from, 0, []domain.Event{opened}); err != nil {
		t.Fatalf("append: %v", err)
	}
	acc, _ := store.Load(ctx, from)
	dep, _ := acc.Deposit(50_00, "")
	_ = store.Append(ctx, from, acc.Version, []domain.Event{dep})

	acc, _ = store.Load(ctx, from)
	tid := domain.NewID()
	deb, err := acc.DebitTransfer(tid, domain.NewID(), 20_00)
	if err != nil {
		t.Fatalf("debit: %v", err)
	}
	if err := store.Append(ctx, from, acc.Version, []domain.Event{deb}); err != nil {
		t.Fatalf("append debit: %v", err)
	}

	// Débito sem crédito nem estorno → pendente.
	pending, err := store.PendingTransferDebits(ctx)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("pending = %d, want 1", len(pending))
	}

	has, err := store.HasTransferEvent(ctx, domain.EventTransferDebited, tid)
	if err != nil || !has {
		t.Fatalf("HasTransferEvent(debited) = %v, %v", has, err)
	}
	has, _ = store.HasTransferEvent(ctx, domain.EventTransferCredited, tid)
	if has {
		t.Fatal("credited must not exist yet")
	}

	// Depois do estorno, sai da lista de pendências.
	acc, _ = store.Load(ctx, from)
	rev, _ := acc.ReverseTransfer(tid, 20_00, "test")
	if err := store.Append(ctx, from, acc.Version, []domain.Event{rev}); err != nil {
		t.Fatalf("append reverse: %v", err)
	}
	pending, _ = store.PendingTransferDebits(ctx)
	if len(pending) != 0 {
		t.Fatalf("pending after reversal = %d, want 0", len(pending))
	}
}
