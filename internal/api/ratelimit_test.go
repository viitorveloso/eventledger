package api

import (
	"testing"
	"time"
)

func TestTokenBucket(t *testing.T) {
	now := time.Unix(1000, 0)
	rl := &RateLimiter{
		buckets: map[string]*bucket{},
		rate:    10, // 10 tokens/s
		burst:   3,
		now:     func() time.Time { return now },
	}

	// Burst inicial: 3 passam, o 4º não.
	for i := 0; i < 3; i++ {
		if !rl.Allow("1.2.3.4") {
			t.Fatalf("request %d should pass within burst", i+1)
		}
	}
	if rl.Allow("1.2.3.4") {
		t.Fatal("4th request should be limited")
	}

	// Outro IP tem balde próprio.
	if !rl.Allow("5.6.7.8") {
		t.Fatal("different IP must have its own bucket")
	}

	// 100ms depois: 1 token reabastecido (10/s).
	now = now.Add(100 * time.Millisecond)
	if !rl.Allow("1.2.3.4") {
		t.Fatal("refill after 100ms should allow one request")
	}
	if rl.Allow("1.2.3.4") {
		t.Fatal("only one token was refilled")
	}

	// Refill nunca passa do burst.
	now = now.Add(time.Hour)
	for i := 0; i < 3; i++ {
		if !rl.Allow("1.2.3.4") {
			t.Fatalf("request %d should pass after long idle", i+1)
		}
	}
	if rl.Allow("1.2.3.4") {
		t.Fatal("bucket must cap at burst")
	}
}
