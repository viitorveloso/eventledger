package saga

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"eventledger/internal/app"
	"eventledger/internal/bus"
	"eventledger/internal/domain"
	"eventledger/internal/eventstore"
)

// pipeline sobe o sistema completo em memória: app + bus + saga.
// É o mesmo wiring do main.go, sem HTTP e sem Postgres.
func pipeline(t *testing.T) (*app.App, *eventstore.Memory, func()) {
	t.Helper()
	store := eventstore.NewMemory()
	b := bus.NewInMemory(4, 256)
	a := app.New(store, b)
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	s := New(a, store, log)

	b.Subscribe(s.Handle)
	ctx, cancel := context.WithCancel(context.Background())
	b.Start(ctx)
	s.Start(ctx, 2)

	return a, store, func() {
		b.Close()
		s.Close()
		cancel()
	}
}

// waitBalance espera a consistência eventual da saga com timeout.
func waitBalance(t *testing.T, store *eventstore.Memory, accID string, want int64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		acc, err := store.Load(context.Background(), accID)
		if err == nil && acc.Balance == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	acc, _ := store.Load(context.Background(), accID)
	t.Fatalf("timeout: balance = %d, want %d", acc.Balance, want)
}

func TestTransferHappyPath(t *testing.T) {
	a, store, stop := pipeline(t)
	defer stop()
	ctx := context.Background()

	from, _ := a.OpenAccount(ctx, "from", 100_00)
	to, _ := a.OpenAccount(ctx, "to", 0)

	tid, err := a.StartTransfer(ctx, from, to, 35_00)
	if err != nil {
		t.Fatalf("transfer: %v", err)
	}

	waitBalance(t, store, to, 35_00)   // crédito chegou
	waitBalance(t, store, from, 65_00) // débito permanece

	ok, _ := store.HasTransferEvent(ctx, domain.EventTransferCredited, tid)
	if !ok {
		t.Fatal("missing transfer.credited event")
	}
}

// O teste da compensação: transferência para conta que NÃO existe.
// O débito é validado e gravado (origem tem saldo), o crédito falha com
// erro de negócio, a saga emite o estorno e o dinheiro volta.
func TestTransferToMissingAccountIsReversed(t *testing.T) {
	a, store, stop := pipeline(t)
	defer stop()
	ctx := context.Background()

	from, _ := a.OpenAccount(ctx, "from", 100_00)
	ghost := domain.NewID() // conta nunca aberta

	tid, err := a.StartTransfer(ctx, from, ghost, 40_00)
	if err != nil {
		t.Fatalf("transfer: %v", err)
	}

	// Dinheiro volta: 100 - 40 + 40 = 100.
	waitBalance(t, store, from, 100_00)

	reversed, _ := store.HasTransferEvent(ctx, domain.EventTransferReversed, tid)
	if !reversed {
		t.Fatal("missing transfer.reversed event")
	}
	credited, _ := store.HasTransferEvent(ctx, domain.EventTransferCredited, tid)
	if credited {
		t.Fatal("ghost account must never be credited")
	}

	// A trilha na origem conta a história completa: aberto, depositado,
	// debitado, estornado — nada é apagado, o erro fica auditável.
	events, _ := store.EventsByAggregate(ctx, from)
	var types []domain.EventType
	for _, e := range events {
		types = append(types, e.Type)
	}
	want := []domain.EventType{
		domain.EventAccountOpened,
		domain.EventMoneyDeposited,
		domain.EventTransferDebited,
		domain.EventTransferReversed,
	}
	if len(types) != len(want) {
		t.Fatalf("event stream = %v, want %v", types, want)
	}
	for i := range want {
		if types[i] != want[i] {
			t.Fatalf("event stream = %v, want %v", types, want)
		}
	}
}

// CatchUp retoma transferências interrompidas: simula um crash entre o
// débito e o crédito (evento gravado mas nunca entregue à saga).
func TestCatchUpResumesPendingTransfer(t *testing.T) {
	store := eventstore.NewMemory()
	nolog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	// Fase 1: sistema "antigo" grava o débito e morre antes da saga agir.
	dead := bus.NewInMemory(1, 16) // nunca é Started: eventos não fluem
	aDead := app.New(store, dead)
	ctx := context.Background()
	from, _ := aDead.OpenAccount(ctx, "from", 50_00)
	to, _ := aDead.OpenAccount(ctx, "to", 0)

	// Injeta o débito direto no store, sem passar pelo bus (crash simulado).
	acc, _ := store.Load(ctx, from)
	ev, err := acc.DebitTransfer(domain.NewID(), to, 20_00)
	if err != nil {
		t.Fatalf("debit: %v", err)
	}
	if err := store.Append(ctx, from, acc.Version, []domain.Event{ev}); err != nil {
		t.Fatalf("append: %v", err)
	}

	// Fase 2: boot novo — CatchUp encontra o débito pendente e conclui.
	b := bus.NewInMemory(2, 64)
	a := app.New(store, b)
	s := New(a, store, nolog)
	b.Subscribe(s.Handle)
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.Start(runCtx)
	s.Start(runCtx, 2)
	defer func() { b.Close(); s.Close() }()

	if err := s.CatchUp(runCtx); err != nil {
		t.Fatalf("catch-up: %v", err)
	}

	waitBalance(t, store, to, 20_00)
	waitBalance(t, store, from, 30_00)
}
