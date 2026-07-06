package eventstore

import (
	"context"
	"errors"
	"testing"

	"eventledger/internal/domain"
)

func TestAppendRejectsAggregateMismatch(t *testing.T) {
	store := NewMemory()
	ctx := context.Background()

	streamID := domain.NewID()
	otherID := domain.NewID()
	opened, err := domain.OpenAccount(otherID, "lucas")
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	err = store.Append(ctx, streamID, 0, []domain.Event{opened})
	if !errors.Is(err, ErrAggregateMismatch) {
		t.Fatalf("err = %v, want ErrAggregateMismatch", err)
	}
}
