// Package closer implements the process-level shutdown harness (spec #52,
// B1 Runtime resilience): wait for SIGTERM/SIGINT, cancel the root context so
// background loops and consumers stop promptly, drain servers with a bounded
// window, then run registered cleanup (close pools, flush OTLP) and return
// the process exit code.
//
// The goal is *prompt* stop, not exclusivity: idempotency and resume
// (ADR-0001/0002) make re-running safe after any restart.
package closer

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// DefaultDrain is the bounded drain window when TAPER_DRAIN is unset.
const DefaultDrain = 15 * time.Second

// DrainWindow returns the drain window from TAPER_DRAIN (e.g. "30s"),
// DefaultDrain when unset, and DefaultDrain with a warning when the value is
// unparseable or non-positive.
func DrainWindow() time.Duration {
	v := os.Getenv("TAPER_DRAIN")
	if v == "" {
		return DefaultDrain
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		log.Printf("closer: invalid TAPER_DRAIN %q, using %s", v, DefaultDrain)
		return DefaultDrain
	}
	return d
}

// CleanupFunc is one registered shutdown step. It receives a context bounded
// by the drain window; open-ended stops (grpc GracefulStop) must race it
// themselves and force-stop on expiry.
type CleanupFunc func(ctx context.Context) error

// Closer is the per-process shutdown harness.
type Closer struct {
	drain time.Duration
	logf  func(format string, args ...any)

	ctx    context.Context
	cancel context.CancelFunc
	sig    chan struct{} // closed on shutdown signal

	mu      sync.Mutex
	defers  []CleanupFunc
	cleaned bool
}

func newCloser(drain time.Duration) *Closer {
	if drain <= 0 {
		drain = DefaultDrain
	}
	c := &Closer{drain: drain, logf: log.Printf, sig: make(chan struct{})}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	return c
}

// New builds a closer with the given drain window, watching SIGTERM/SIGINT.
// A second signal forces an immediate exit.
func New(drain time.Duration) *Closer {
	c := newCloser(drain)
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		select {
		case <-ch:
			c.signal()
			<-ch // a second signal means the operator wants out now
			c.logf("closer: second signal, exiting immediately")
			os.Exit(1)
		case <-c.ctx.Done():
		}
	}()
	return c
}

// NewWithSignal is the test seam: like New but "signal" is the closing of
// fired, and no OS signal handler is installed.
func NewWithSignal(drain time.Duration, fired <-chan struct{}) *Closer {
	c := newCloser(drain)
	go func() {
		select {
		case <-fired:
			c.signal()
		case <-c.ctx.Done():
		}
	}()
	return c
}

// signal starts the shutdown: cancel the root context first so loops and
// consumers stop promptly, then release WaitChan.
func (c *Closer) signal() {
	c.logf("closer: shutdown signal received, draining (window %s)", c.drain)
	c.cancel()
	close(c.sig)
}

// Context returns the root context for background loops and consumers. It is
// cancelled as the first act of shutdown so everything watching it stops
// promptly.
func (c *Closer) Context() context.Context { return c.ctx }

// WaitChan is closed as soon as the shutdown signal is handled (the root
// context is already cancelled by then). Binaries select on it alongside
// their blocking serve/consume call to know shutdown has begun.
func (c *Closer) WaitChan() <-chan struct{} { return c.sig }

// Drain returns the configured drain window.
func (c *Closer) Drain() time.Duration { return c.drain }

// Defer registers a cleanup run during Cleanup, last-registered first.
// Register in boot order (observability first, servers last) so servers stop
// before the pools they use are closed.
func (c *Closer) Defer(f CleanupFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.defers = append(c.defers, f)
}

// DeferFirst registers a cleanup to run before everything registered via
// Defer. Servers use it: they must stop accepting before pools and
// connections registered after them are closed, while still having been
// booted after observability.

func (c *Closer) DeferFirst(f CleanupFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.defers = append([]CleanupFunc{f}, c.defers...)
}

// Wait blocks until the shutdown signal, then cleans up and returns the
// process exit code: 0 on a clean drain, 1 when any cleanup step failed (for
// example the drain window expired).
func (c *Closer) Wait() int {
	<-c.sig
	return c.Cleanup()
}

// Cleanup cancels the root context and runs registered cleanups LIFO under a
// single context bounded by the drain window. Idempotent; the first call
// decides the exit code.
func (c *Closer) Cleanup() int {
	c.mu.Lock()
	if c.cleaned {
		c.mu.Unlock()
		return 0
	}
	c.cleaned = true
	defers := c.defers
	c.defers = nil
	c.mu.Unlock()

	c.cancel()

	code := 0
	dctx, cancel := context.WithTimeout(context.Background(), c.drain)
	defer cancel()
	for i := len(defers) - 1; i >= 0; i-- {
		if err := defers[i](dctx); err != nil {
			if dctx.Err() != nil {
				c.logf("closer: cleanup %d/%d exceeded drain window: %v", i+1, len(defers), err)
			} else {
				c.logf("closer: cleanup %d/%d failed: %v", i+1, len(defers), err)
			}
			code = 1
		}
	}
	if code == 0 {
		c.logf("closer: shutdown complete")
	}
	return code
}
