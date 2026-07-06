// Package bus define a porta de mensageria e uma implementação in-process.
//
// A implementação espelha as garantias do Kafka que importam para o domínio:
//   - particionamento por aggregate_id → ordem garantida POR CONTA
//   - fan-out para múltiplos consumidores (projetor, saga)
//   - entrega assíncrona com semântica at-least-once (eventos podem ser
//     descartados num shutdown e reentregues pelo catch-up do próximo boot;
//     consumidores DEVEM ser idempotentes)
//
// Em produção, esta interface seria implementada por um producer Kafka com
// key = aggregate_id — o resto do sistema não muda uma linha.
package bus

import (
	"context"
	"hash/fnv"
	"sync"

	"eventledger/internal/domain"
)

type Handler func(ctx context.Context, e domain.Event)

type Bus interface {
	Publish(e domain.Event)
	Subscribe(h Handler) // deve ser chamado antes de Start
}

type InMemory struct {
	partitions []chan domain.Event
	handlers   []Handler
	wg         sync.WaitGroup
	done       chan struct{}
	closeOnce  sync.Once
	started    bool
	mu         sync.Mutex
}

// NewInMemory cria o bus com N partições e buffer por partição.
// Buffer generoso reduz backpressure sobre os producers; num broker real
// esse desacoplamento é responsabilidade da infraestrutura.
func NewInMemory(partitions, buffer int) *InMemory {
	b := &InMemory{
		partitions: make([]chan domain.Event, partitions),
		done:       make(chan struct{}),
	}
	for i := range b.partitions {
		b.partitions[i] = make(chan domain.Event, buffer)
	}
	return b
}

func (b *InMemory) Subscribe(h Handler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.started {
		panic("bus: Subscribe after Start")
	}
	b.handlers = append(b.handlers, h)
}

// Start sobe um worker por partição. Cada worker entrega os eventos da sua
// partição a todos os handlers, em sequência — preservando a ordem por conta.
func (b *InMemory) Start(ctx context.Context) {
	b.mu.Lock()
	b.started = true
	b.mu.Unlock()

	// Propaga o cancelamento do app para o bus.
	go func() {
		<-ctx.Done()
		b.signalDone()
	}()

	for _, ch := range b.partitions {
		b.wg.Add(1)
		go func(ch chan domain.Event) {
			defer b.wg.Done()
			for {
				select {
				case e := <-ch:
					b.deliver(ctx, e)
				case <-b.done:
					// Drena o buffer restante (best-effort) e encerra.
					for {
						select {
						case e := <-ch:
							b.deliver(ctx, e)
						default:
							return
						}
					}
				}
			}
		}(ch)
	}
}

func (b *InMemory) deliver(ctx context.Context, e domain.Event) {
	for _, h := range b.handlers {
		h(ctx, e)
	}
}

// Publish roteia o evento para a partição do seu agregado.
// Mesmo hash → mesma partição → mesmo worker → ordem preservada.
//
// Os channels de dados nunca são fechados — shutdown é sinalizado por done.
// Publish durante o shutdown descarta o evento: como ele JÁ está no event
// store, o catch-up do próximo boot o reentrega (at-least-once por design).
func (b *InMemory) Publish(e domain.Event) {
	h := fnv.New32a()
	h.Write([]byte(e.AggregateID))
	idx := int(h.Sum32()) % len(b.partitions)

	select {
	case b.partitions[idx] <- e:
	case <-b.done:
	}
}

// Close sinaliza o shutdown e espera os workers drenarem suas partições.
func (b *InMemory) Close() {
	b.signalDone()
	b.wg.Wait()
}

func (b *InMemory) signalDone() {
	b.closeOnce.Do(func() { close(b.done) })
}
