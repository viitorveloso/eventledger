package bus

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"eventledger/internal/domain"
)

// A garantia central do bus: eventos do MESMO agregado chegam em ordem,
// mesmo com várias partições e publicação concorrente de vários agregados.
// Rode com -race.
func TestOrderPreservedPerAggregate(t *testing.T) {
	const aggregates = 10
	const eventsPerAggregate = 200

	b := NewInMemory(4, 64)

	var mu sync.Mutex
	received := make(map[string][]int64)

	b.Subscribe(func(_ context.Context, e domain.Event) {
		mu.Lock()
		received[e.AggregateID] = append(received[e.AggregateID], e.Version)
		mu.Unlock()
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.Start(ctx)

	ids := make([]string, aggregates)
	for i := range ids {
		ids[i] = domain.NewID()
	}

	// Um publisher por agregado, todos ao mesmo tempo.
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for v := int64(1); v <= eventsPerAggregate; v++ {
				b.Publish(domain.Event{
					ID: domain.NewID(), AggregateID: id, Version: v,
					Type: domain.EventMoneyDeposited, Data: json.RawMessage(`{}`),
				})
			}
		}(id)
	}
	wg.Wait()
	b.Close() // drena tudo antes de verificar

	for _, id := range ids {
		got := received[id]
		if len(got) != eventsPerAggregate {
			t.Fatalf("aggregate %s: received %d events, want %d", id, len(got), eventsPerAggregate)
		}
		for i, v := range got {
			if v != int64(i+1) {
				t.Fatalf("aggregate %s: position %d has version %d — out of order", id, i, v)
			}
		}
	}
}

// Fan-out: todos os subscribers recebem todos os eventos.
func TestFanOutToAllSubscribers(t *testing.T) {
	b := NewInMemory(2, 16)

	var mu sync.Mutex
	counts := [2]int{}
	for i := 0; i < 2; i++ {
		i := i
		b.Subscribe(func(_ context.Context, _ domain.Event) {
			mu.Lock()
			counts[i]++
			mu.Unlock()
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.Start(ctx)

	const n = 50
	for i := 0; i < n; i++ {
		b.Publish(domain.Event{
			ID: domain.NewID(), AggregateID: domain.NewID(), Version: 1,
			Type: domain.EventMoneyDeposited, Data: json.RawMessage(`{}`),
		})
	}
	b.Close()

	if counts[0] != n || counts[1] != n {
		t.Fatalf("fan-out broken: counts = %v, want [%d %d]", counts, n, n)
	}
}
