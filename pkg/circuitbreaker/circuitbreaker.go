// Package circuitbreaker implements the Fail-Fast Policy (CONTEXT.md):
// when a downstream dependency is failing, calls fail immediately with an
// error instead of queuing against a dead dependency.
package circuitbreaker

import (
	"errors"
	"sync"
	"time"
)

// ErrOpen is returned immediately while the breaker is open (fail fast).
var ErrOpen = errors.New("circuit breaker open: dependency unavailable")

// States of the breaker.
const (
	Closed   = "closed"   // normal operation
	Open     = "open"     // failing fast after consecutive failures
	HalfOpen = "half-open" // letting a probe request through
)

// Config tunes the breaker.
type Config struct {
	// Consecutive failures before opening. Defaults to 5.
	FailureThreshold int
	// How long the breaker stays open before allowing a probe. Defaults to 10s.
	Cooldown time.Duration
}

// Breaker is a goroutine-safe circuit breaker for one downstream dependency.
type Breaker struct {
	mu      sync.Mutex
	cfg     Config
	state   string
	failures int
	openedAt time.Time
}

// New creates a closed breaker.
func New(cfg Config) *Breaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = 10 * time.Second
	}
	return &Breaker{cfg: cfg, state: Closed}
}

// Allow reports whether a call may proceed. It transitions Open -> HalfOpen
// once the cooldown has elapsed.
func (b *Breaker) Allow() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case Closed:
		return nil
	case Open:
		if time.Since(b.openedAt) >= b.cfg.Cooldown {
			b.state = HalfOpen
			return nil
		}
		return ErrOpen
	default: // HalfOpen: allow the probe
		return nil
	}
}

// RecordSuccess records a successful call: any state resets to Closed.
func (b *Breaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state = Closed
	b.failures = 0
}

// RecordFailure records a failed call. In HalfOpen, a single failure reopens.
func (b *Breaker) RecordFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case HalfOpen:
		b.state = Open
		b.openedAt = time.Now()
	case Closed:
		b.failures++
		if b.failures >= b.cfg.FailureThreshold {
			b.state = Open
			b.openedAt = time.Now()
		}
	}
}

// State returns the current state name (for observability).
func (b *Breaker) State() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}
