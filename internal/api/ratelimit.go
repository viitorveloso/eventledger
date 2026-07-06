package api

import (
	"sync"
	"time"
)

// RateLimiter implementa Token Bucket por chave (IP do cliente).
// Refill é lazy: calculado a partir do tempo decorrido no momento do Allow,
// sem goroutine de background por bucket.
type RateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	rate    float64 // tokens por segundo
	burst   float64 // capacidade do balde
	now     func() time.Time
}

type bucket struct {
	tokens float64
	last   time.Time
}

func NewRateLimiter(ratePerSecond, burst float64) *RateLimiter {
	rl := &RateLimiter{
		buckets: make(map[string]*bucket),
		rate:    ratePerSecond,
		burst:   burst,
		now:     time.Now,
	}
	go rl.janitor()
	return rl
}

func (rl *RateLimiter) Allow(key string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := rl.now()
	b, ok := rl.buckets[key]
	if !ok {
		b = &bucket{tokens: rl.burst, last: now}
		rl.buckets[key] = b
	}

	elapsed := now.Sub(b.last).Seconds()
	b.tokens = min(rl.burst, b.tokens+elapsed*rl.rate)
	b.last = now

	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

// janitor remove buckets inativos para o mapa não crescer sem limite.
func (rl *RateLimiter) janitor() {
	for range time.Tick(time.Minute) {
		rl.mu.Lock()
		cutoff := rl.now().Add(-5 * time.Minute)
		for k, b := range rl.buckets {
			if b.last.Before(cutoff) {
				delete(rl.buckets, k)
			}
		}
		rl.mu.Unlock()
	}
}
