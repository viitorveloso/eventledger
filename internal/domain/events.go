// Package domain contém o núcleo de negócio: agregados, eventos e regras.
// Não depende de banco, HTTP ou mensageria — é Go puro e 100% testável.
package domain

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"time"
)

type EventType string

const (
	EventAccountOpened    EventType = "account.opened"
	EventMoneyDeposited   EventType = "money.deposited"
	EventMoneyWithdrawn   EventType = "money.withdrawn"
	EventTransferDebited  EventType = "transfer.debited"
	EventTransferCredited EventType = "transfer.credited"
	EventTransferReversed EventType = "transfer.reversed"
)

// Event é o envelope imutável gravado no event store.
// Seq é a ordem global atribuída pelo store na gravação;
// Version é a posição do evento dentro do stream do agregado.
type Event struct {
	ID          string          `json:"id"`
	AggregateID string          `json:"aggregate_id"`
	Version     int64           `json:"version"`
	Type        EventType       `json:"type"`
	Data        json.RawMessage `json:"data"`
	OccurredAt  time.Time       `json:"occurred_at"`
	Seq         int64           `json:"seq,omitempty"`
}

// ---- Payloads ----

type AccountOpened struct {
	Owner string `json:"owner"`
}

type MoneyDeposited struct {
	Amount      int64  `json:"amount"`
	Description string `json:"description,omitempty"`
}

type MoneyWithdrawn struct {
	Amount      int64  `json:"amount"`
	Description string `json:"description,omitempty"`
}

type TransferDebited struct {
	TransferID string `json:"transfer_id"`
	To         string `json:"to"`
	Amount     int64  `json:"amount"`
}

type TransferCredited struct {
	TransferID string `json:"transfer_id"`
	From       string `json:"from"`
	Amount     int64  `json:"amount"`
}

type TransferReversed struct {
	TransferID string `json:"transfer_id"`
	Amount     int64  `json:"amount"`
	Reason     string `json:"reason"`
}

// NewEvent monta um envelope a partir de um payload tipado.
func NewEvent(aggregateID string, version int64, typ EventType, payload any) (Event, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return Event{}, fmt.Errorf("marshal payload %s: %w", typ, err)
	}
	return Event{
		ID:          NewID(),
		AggregateID: aggregateID,
		Version:     version,
		Type:        typ,
		Data:        data,
		OccurredAt:  time.Now().UTC(),
	}, nil
}

// NewID gera um UUID v4 usando apenas a stdlib.
func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("crypto/rand failed: %v", err)) // sem entropia, nada é seguro
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant RFC 4122
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
