package app

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"eventledger/internal/bus"
	"eventledger/internal/domain"
	"eventledger/internal/eventstore"
)

// nopBus descarta eventos: estes testes exercitam só o command side.
type nopBus struct{}

func (nopBus) Publish(domain.Event)  {}
func (nopBus) Subscribe(bus.Handler) {}

func newTestApp() (*App, *eventstore.Memory) {
	store := eventstore.NewMemory()
	return New(store, nopBus{}), store
}

// TestConcurrentWithdrawalsNeverOverdraw é a prova central do design:
// N goroutines disputando a mesma conta, cada uma tentando sacar.
// O optimistic locking garante que a soma dos saques bem-sucedidos
// nunca excede o saldo — sem lock pessimista, sem SELECT FOR UPDATE.
//
// Rode com -race: também prova ausência de data race no caminho quente.
func TestConcurrentWithdrawalsNeverOverdraw(t *testing.T) {
	a, _ := newTestApp()
	ctx := context.Background()

	const initial = 100_00 // R$ 100,00
	const each = 10_00     // R$ 10,00 → no máximo 10 saques cabem

	accID, err := a.OpenAccount(ctx, "lucas", initial)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	const workers = 50
	var ok, insufficient, conflicted atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := a.Withdraw(ctx, accID, each, "concurrent")
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, domain.ErrInsufficientFunds):
				insufficient.Add(1)
			case errors.Is(err, ErrTooManyConflicts):
				conflicted.Add(1)
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	acc, err := a.store.Load(ctx, accID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	// Invariantes de dinheiro:
	if acc.Balance < 0 {
		t.Fatalf("OVERDRAW: balance = %d", acc.Balance)
	}
	if want := int64(initial - ok.Load()*each); acc.Balance != want {
		t.Fatalf("ledger mismatch: balance = %d, want %d (ok=%d)", acc.Balance, want, ok.Load())
	}
	if ok.Load() > initial/each {
		t.Fatalf("more successful withdrawals (%d) than the balance allows (%d)", ok.Load(), initial/each)
	}
	if ok.Load() == 0 {
		t.Fatal("no withdrawal succeeded; retry loop is broken")
	}
	t.Logf("ok=%d insufficient=%d conflicted=%d balance=%d",
		ok.Load(), insufficient.Load(), conflicted.Load(), acc.Balance)
}

// Depósitos concorrentes: todo depósito aceito precisa estar no saldo.
// Aqui não há regra de saldo para falhar — só a contabilidade importa.
func TestConcurrentDepositsAllAccounted(t *testing.T) {
	a, _ := newTestApp()
	ctx := context.Background()

	accID, err := a.OpenAccount(ctx, "lucas", 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	const workers = 30
	var ok atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := a.Deposit(ctx, accID, 1_00, ""); err == nil {
				ok.Add(1)
			}
		}()
	}
	wg.Wait()

	acc, _ := a.store.Load(ctx, accID)
	if acc.Balance != ok.Load()*1_00 {
		t.Fatalf("balance %d != successful deposits %d", acc.Balance, ok.Load()*1_00)
	}
	if ok.Load() == 0 {
		t.Fatal("no deposit succeeded")
	}
}

func TestTransferValidatesSynchronously(t *testing.T) {
	a, _ := newTestApp()
	ctx := context.Background()

	from, _ := a.OpenAccount(ctx, "from", 10_00)
	to, _ := a.OpenAccount(ctx, "to", 0)

	// Sem saldo: rejeitado NA HORA — nada de 202 para depois falhar.
	if _, err := a.StartTransfer(ctx, from, to, 10_01); !errors.Is(err, domain.ErrInsufficientFunds) {
		t.Fatalf("want ErrInsufficientFunds, got %v", err)
	}
	// Mesma conta: rejeitado.
	if _, err := a.StartTransfer(ctx, from, from, 1_00); !errors.Is(err, domain.ErrSameAccount) {
		t.Fatalf("want ErrSameAccount, got %v", err)
	}
	// Conta origem inexistente: rejeitado.
	if _, err := a.StartTransfer(ctx, domain.NewID(), to, 1_00); !errors.Is(err, domain.ErrAccountNotFound) {
		t.Fatalf("want ErrAccountNotFound, got %v", err)
	}
}

// CreditTransfer e ReverseTransfer precisam ser idempotentes por
// transfer_id: sob at-least-once, a segunda entrega vira no-op.
func TestSagaStepsAreIdempotent(t *testing.T) {
	a, store := newTestApp()
	ctx := context.Background()

	from, _ := a.OpenAccount(ctx, "from", 100_00)
	to, _ := a.OpenAccount(ctx, "to", 0)

	tid, err := a.StartTransfer(ctx, from, to, 40_00)
	if err != nil {
		t.Fatalf("transfer: %v", err)
	}

	for i := 0; i < 3; i++ { // entrega duplicada simulada
		if err := a.CreditTransfer(ctx, to, tid, from, 40_00); err != nil {
			t.Fatalf("credit #%d: %v", i, err)
		}
	}
	toAcc, _ := store.Load(ctx, to)
	if toAcc.Balance != 40_00 {
		t.Fatalf("destination credited %d, want 4000 (duplicate applied?)", toAcc.Balance)
	}

	for i := 0; i < 3; i++ {
		if err := a.ReverseTransfer(ctx, from, tid, 40_00, "test"); err != nil {
			t.Fatalf("reverse #%d: %v", i, err)
		}
	}
	fromAcc, _ := store.Load(ctx, from)
	// 100 - 40 (débito) + 40 (um único estorno) = 100
	if fromAcc.Balance != 100_00 {
		t.Fatalf("origin balance %d, want 10000 (duplicate reversal applied?)", fromAcc.Balance)
	}
}
