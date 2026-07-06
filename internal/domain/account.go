package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Erros de negócio. A API os traduz para códigos HTTP;
// o resto do sistema os trata por errors.Is.
var (
	ErrAccountClosed     = errors.New("account is closed")
	ErrAccountNotFound   = errors.New("account not found")
	ErrInsufficientFunds = errors.New("insufficient funds")
	ErrInvalidAmount     = errors.New("amount must be positive")
	ErrOwnerRequired     = errors.New("owner is required")
	ErrSameAccount       = errors.New("transfer to the same account")
	ErrAmountTooLarge    = errors.New("amount exceeds per-operation limit")
)

// MaxOperationAmount limita uma única operação (R$ 1.000.000,00 em centavos).
// Regra simples de sanidade; limites reais viriam de um motor de risco.
const MaxOperationAmount int64 = 100_000_000

// Account é o agregado. Todo o estado é derivado do replay dos eventos:
// nenhum campo é escrito diretamente — apenas via Apply.
type Account struct {
	ID      string `json:"id"`
	Owner   string `json:"owner"`
	Balance int64  `json:"balance"` // centavos (int64 evita float em dinheiro)
	Open    bool   `json:"open"`
	Version int64  `json:"version"` // último version aplicado (optimistic locking)
}

// Apply muta o estado a partir de um evento já persistido.
// Nunca valida regra de negócio: eventos são fatos, já aconteceram.
func (a *Account) Apply(e Event) error {
	switch e.Type {
	case EventAccountOpened:
		var p AccountOpened
		if err := json.Unmarshal(e.Data, &p); err != nil {
			return fmt.Errorf("apply %s: %w", e.Type, err)
		}
		a.ID = e.AggregateID
		a.Owner = p.Owner
		a.Open = true
	case EventMoneyDeposited:
		var p MoneyDeposited
		if err := json.Unmarshal(e.Data, &p); err != nil {
			return fmt.Errorf("apply %s: %w", e.Type, err)
		}
		a.Balance += p.Amount
	case EventMoneyWithdrawn:
		var p MoneyWithdrawn
		if err := json.Unmarshal(e.Data, &p); err != nil {
			return fmt.Errorf("apply %s: %w", e.Type, err)
		}
		a.Balance -= p.Amount
	case EventTransferDebited:
		var p TransferDebited
		if err := json.Unmarshal(e.Data, &p); err != nil {
			return fmt.Errorf("apply %s: %w", e.Type, err)
		}
		a.Balance -= p.Amount
	case EventTransferCredited:
		var p TransferCredited
		if err := json.Unmarshal(e.Data, &p); err != nil {
			return fmt.Errorf("apply %s: %w", e.Type, err)
		}
		a.Balance += p.Amount
	case EventTransferReversed:
		var p TransferReversed
		if err := json.Unmarshal(e.Data, &p); err != nil {
			return fmt.Errorf("apply %s: %w", e.Type, err)
		}
		a.Balance += p.Amount // estorno devolve o valor debitado
	default:
		return fmt.Errorf("unknown event type %q", e.Type)
	}
	a.Version = e.Version
	return nil
}

// Replay reconstrói um agregado a partir do stream (opcionalmente sobre um snapshot).
func Replay(base Account, events []Event) (Account, error) {
	a := base
	for _, e := range events {
		if err := a.Apply(e); err != nil {
			return Account{}, err
		}
	}
	return a, nil
}

// ---- Decisões (command handlers do agregado) ----
// Validam regras contra o estado atual e retornam eventos.
// NÃO aplicam os eventos: quem orquestra é a camada de aplicação,
// depois que o store confirmar a gravação.

func OpenAccount(id, owner string) (Event, error) {
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return Event{}, ErrOwnerRequired
	}
	return NewEvent(id, 1, EventAccountOpened, AccountOpened{Owner: owner})
}

func (a *Account) Deposit(amount int64, description string) (Event, error) {
	if err := a.guard(amount); err != nil {
		return Event{}, err
	}
	return NewEvent(a.ID, a.Version+1, EventMoneyDeposited,
		MoneyDeposited{Amount: amount, Description: description})
}

func (a *Account) Withdraw(amount int64, description string) (Event, error) {
	if err := a.guard(amount); err != nil {
		return Event{}, err
	}
	if a.Balance < amount {
		return Event{}, ErrInsufficientFunds
	}
	return NewEvent(a.ID, a.Version+1, EventMoneyWithdrawn,
		MoneyWithdrawn{Amount: amount, Description: description})
}

// DebitTransfer é o primeiro passo da saga: valida saldo AQUI, de forma
// síncrona, antes de qualquer coisa ser aceita. Transferência sem saldo
// nunca entra no sistema.
func (a *Account) DebitTransfer(transferID, to string, amount int64) (Event, error) {
	if err := a.guard(amount); err != nil {
		return Event{}, err
	}
	if a.ID == to {
		return Event{}, ErrSameAccount
	}
	if a.Balance < amount {
		return Event{}, ErrInsufficientFunds
	}
	return NewEvent(a.ID, a.Version+1, EventTransferDebited,
		TransferDebited{TransferID: transferID, To: to, Amount: amount})
}

func (a *Account) CreditTransfer(transferID, from string, amount int64) (Event, error) {
	if err := a.guard(amount); err != nil {
		return Event{}, err
	}
	return NewEvent(a.ID, a.Version+1, EventTransferCredited,
		TransferCredited{TransferID: transferID, From: from, Amount: amount})
}

// ReverseTransfer compensa um débito cujo crédito falhou (saga compensation).
func (a *Account) ReverseTransfer(transferID string, amount int64, reason string) (Event, error) {
	if amount <= 0 {
		return Event{}, ErrInvalidAmount
	}
	if a.ID == "" {
		return Event{}, ErrAccountNotFound
	}
	return NewEvent(a.ID, a.Version+1, EventTransferReversed,
		TransferReversed{TransferID: transferID, Amount: amount, Reason: reason})
}

func (a *Account) guard(amount int64) error {
	if a.ID == "" {
		return ErrAccountNotFound
	}
	if !a.Open {
		return ErrAccountClosed
	}
	if amount <= 0 {
		return ErrInvalidAmount
	}
	if amount > MaxOperationAmount {
		return ErrAmountTooLarge
	}
	return nil
}
