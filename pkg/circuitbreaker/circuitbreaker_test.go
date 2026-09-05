package circuitbreaker

import (
	"errors"
	"testing"
	"time"
)

func TestBreakerStartsClosed(t *testing.T) {
	b := New(Config{})
	if err := b.Allow(); err != nil {
		t.Fatalf("expected allow in closed state, got %v", err)
	}
	if b.State() != Closed {
		t.Fatalf("expected closed, got %s", b.State())
	}
}

func TestBreakerOpensAfterThreshold(t *testing.T) {
	b := New(Config{FailureThreshold: 3})
	for i := 0; i < 3; i++ {
		b.RecordFailure()
	}
	if b.State() != Open {
		t.Fatalf("expected open after %d failures, got %s", 3, b.State())
	}
	if !errors.Is(b.Allow(), ErrOpen) {
		t.Fatalf("expected ErrOpen, got %v", b.Allow())
	}
}

func TestBreakerStaysClosedBelowThreshold(t *testing.T) {
	b := New(Config{FailureThreshold: 3})
	b.RecordFailure()
	b.RecordFailure()
	if err := b.Allow(); err != nil {
		t.Fatalf("expected allow below threshold, got %v", err)
	}
	// A success resets the counter.
	b.RecordSuccess()
	b.RecordFailure()
	b.RecordFailure()
	if err := b.Allow(); err != nil {
		t.Fatalf("expected allow after success reset, got %v", err)
	}
}

func TestBreakerHalfOpenProbe(t *testing.T) {
	b := New(Config{FailureThreshold: 1, Cooldown: 10 * time.Millisecond})
	b.RecordFailure()
	if b.State() != Open {
		t.Fatalf("expected open, got %s", b.State())
	}
	time.Sleep(15 * time.Millisecond)
	if err := b.Allow(); err != nil {
		t.Fatalf("expected probe allowed after cooldown, got %v", err)
	}
	if b.State() != HalfOpen {
		t.Fatalf("expected half-open, got %s", b.State())
	}
}

func TestBreakerHalfOpenSuccessCloses(t *testing.T) {
	b := New(Config{FailureThreshold: 1, Cooldown: 5 * time.Millisecond})
	b.RecordFailure()
	time.Sleep(10 * time.Millisecond)
	_ = b.Allow() // probe
	b.RecordSuccess()
	if b.State() != Closed {
		t.Fatalf("expected closed after successful probe, got %s", b.State())
	}
	if err := b.Allow(); err != nil {
		t.Fatalf("expected allow, got %v", err)
	}
}

func TestBreakerHalfOpenFailureReopens(t *testing.T) {
	b := New(Config{FailureThreshold: 1, Cooldown: 5 * time.Millisecond})
	b.RecordFailure()
	time.Sleep(10 * time.Millisecond)
	_ = b.Allow() // probe
	b.RecordFailure()
	if b.State() != Open {
		t.Fatalf("expected reopen after failed probe, got %s", b.State())
	}
	if !errors.Is(b.Allow(), ErrOpen) {
		t.Fatalf("expected ErrOpen immediately after reopen, got %v", b.Allow())
	}
}

func TestBreakerOpenFailsFastWithoutWaiting(t *testing.T) {
	b := New(Config{FailureThreshold: 1, Cooldown: time.Hour})
	b.RecordFailure()
	start := time.Now()
	_ = b.Allow()
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Allow() took %v while open; expected immediate fail-fast", elapsed)
	}
}
