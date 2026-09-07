// Package ratelimit provides an in-process per-tenant token bucket for the
// gateway. One bucket per tenant: bursts are absorbed up to the bucket
// capacity, sustained traffic is capped at the refill rate, and tenants are
// isolated from each other. Deliberately not distributed — Redis stays out
// until a multi-gateway deployment earns it.
package ratelimit

import (
	"sync"
	"time"
)

// Decision is the result of a Take attempt.
type Decision struct {
	// Allowed is true when a token was consumed.
	Allowed bool
	// RetryAfter is how long until a token is available next; meaningful
	// only when Allowed is false.
	RetryAfter time.Duration
}

// Bucket is one tenant's token bucket.
type bucket struct {
	tokens float64
	last   time.Time
}

// Limiter hands out one bucket per tenant.
type Limiter struct {
	mu     sync.Mutex
	rate   float64 // tokens per second
	burst  float64 // bucket capacity
	buckets map[string]*bucket
	now    func() time.Time
}

// New creates a limiter refilling at rate tokens per second with burst
// capacity. rate or burst <= 0 disables limiting (every Take succeeds).
func New(rate float64, burst float64) *Limiter {
	return &Limiter{
		rate:    rate,
		burst:   burst,
		buckets: make(map[string]*bucket),
		now:     time.Now,
	}
}

// Take consumes one token for tenant, refilling the bucket first.
func (l *Limiter) Take(tenant string) Decision {
	if l.rate <= 0 || l.burst <= 0 {
		return Decision{Allowed: true}
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	b, ok := l.buckets[tenant]
	if !ok {
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[tenant] = b
	}
	elapsed := now.Sub(b.last).Seconds()
	b.tokens = min(l.burst, b.tokens+elapsed*l.rate)
	b.last = now

	if b.tokens < 1 {
		needed := (1 - b.tokens) / l.rate
		return Decision{Allowed: false, RetryAfter: time.Duration(needed * float64(time.Second))}
	}
	b.tokens--
	return Decision{Allowed: true}
}
