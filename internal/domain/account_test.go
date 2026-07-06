package domain

import (
	"errors"
	"testing"
)

func openTestAccount(t *testing.T, deposit int64) Account {
	t.Helper()
	opened, err := OpenAccount(NewID(), "lucas")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	acc, err := Replay(Account{}, []Event{opened})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if deposit > 0 {
		dep, err := acc.Deposit(deposit, "seed")
		if err != nil {
			t.Fatalf("deposit: %v", err)
		}
		if err := acc.Apply(dep); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	return acc
}

func TestWithdrawRules(t *testing.T) {
	tests := []struct {
		name    string
		balance int64
		amount  int64
		wantErr error
	}{
		{"happy path", 100_00, 40_00, nil},
		{"exact balance", 100_00, 100_00, nil},
		{"insufficient funds", 100_00, 100_01, ErrInsufficientFunds},
		{"zero amount", 100_00, 0, ErrInvalidAmount},
		{"negative amount", 100_00, -5_00, ErrInvalidAmount},
		{"above per-op limit", 100_00, MaxOperationAmount + 1, ErrAmountTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			acc := openTestAccount(t, tt.balance)
			_, err := acc.Withdraw(tt.amount, "")
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("got err %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestDebitTransferRules(t *testing.T) {
	acc := openTestAccount(t, 50_00)

	if _, err := acc.DebitTransfer(NewID(), acc.ID, 10_00); !errors.Is(err, ErrSameAccount) {
		t.Fatalf("same account: got %v", err)
	}
	if _, err := acc.DebitTransfer(NewID(), NewID(), 50_01); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("insufficient: got %v", err)
	}
	ev, err := acc.DebitTransfer(NewID(), NewID(), 50_00)
	if err != nil {
		t.Fatalf("debit: %v", err)
	}
	if err := acc.Apply(ev); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if acc.Balance != 0 {
		t.Fatalf("balance = %d, want 0", acc.Balance)
	}
}

func TestReplayIsDeterministic(t *testing.T) {
	acc := openTestAccount(t, 0)
	var events []Event

	amounts := []int64{100_00, -30_00, 25_50, -10_00}
	for _, amt := range amounts {
		var ev Event
		var err error
		if amt > 0 {
			ev, err = acc.Deposit(amt, "")
		} else {
			ev, err = acc.Withdraw(-amt, "")
		}
		if err != nil {
			t.Fatalf("decide: %v", err)
		}
		if err := acc.Apply(ev); err != nil {
			t.Fatalf("apply: %v", err)
		}
		events = append(events, ev)
	}

	// Estado reconstruído do zero tem que bater com o estado incremental.
	base := Account{}
	opened, _ := OpenAccount(acc.ID, "lucas")
	opened.AggregateID = acc.ID
	rebuilt, err := Replay(base, append([]Event{{
		ID: opened.ID, AggregateID: acc.ID, Version: 1,
		Type: EventAccountOpened, Data: opened.Data, OccurredAt: opened.OccurredAt,
	}}, events...))
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if rebuilt.Balance != acc.Balance {
		t.Fatalf("rebuilt balance %d != live balance %d", rebuilt.Balance, acc.Balance)
	}
	if rebuilt.Balance != 85_50 {
		t.Fatalf("balance = %d, want 8550", rebuilt.Balance)
	}
	if rebuilt.Version != acc.Version {
		t.Fatalf("rebuilt version %d != live version %d", rebuilt.Version, acc.Version)
	}
}

func TestApplyRejectsUnknownEvent(t *testing.T) {
	acc := openTestAccount(t, 0)
	err := acc.Apply(Event{Type: "alien.event", Data: []byte(`{}`)})
	if err == nil {
		t.Fatal("expected error for unknown event type")
	}
}
