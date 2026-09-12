package closer

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDrainWindow(t *testing.T) {
	t.Setenv("TAPER_DRAIN", "")
	if got := DrainWindow(); got != DefaultDrain {
		t.Fatalf("unset: got %v, want %v", got, DefaultDrain)
	}
	t.Setenv("TAPER_DRAIN", "2s")
	if got := DrainWindow(); got != 2*time.Second {
		t.Fatalf("2s: got %v, want 2s", got)
	}
	for _, bad := range []string{"abc", "0s", "-5s"} {
		t.Setenv("TAPER_DRAIN", bad)
		if got := DrainWindow(); got != DefaultDrain {
			t.Fatalf("bad %q: got %v, want default", bad, got)
		}
	}
}

// NewWithSignal closes the fired channel: shutdown starts, the root context
// is cancelled, and Wait returns 0 after cleanups ran.
func TestWaitRunsCleanupsAndCancelsContext(t *testing.T) {
	fired := make(chan struct{})
	c := NewWithSignal(2*time.Second, fired)

	var order []string
	c.Defer(func(context.Context) error { order = append(order, "first"); return nil })
	c.Defer(func(context.Context) error { order = append(order, "second"); return nil })

	// The root context must be usable by background loops.
	if c.Context().Err() != nil {
		t.Fatal("root context cancelled before signal")
	}
	close(fired)

	if code := c.Wait(); code != 0 {
		t.Fatalf("Wait: got exit %d, want 0", code)
	}
	if c.Context().Err() == nil {
		t.Fatal("root context not cancelled after signal")
	}
	if len(order) != 2 || order[0] != "second" || order[1] != "first" {
		t.Fatalf("cleanup order: got %v, want LIFO [second first]", order)
	}
}

// A failing cleanup flips the exit code to 1.
func TestWaitExitCodeOnCleanupFailure(t *testing.T) {
	fired := make(chan struct{})
	c := NewWithSignal(2*time.Second, fired)
	c.Defer(func(context.Context) error { return errors.New("boom") })
	close(fired)
	if code := c.Wait(); code != 1 {
		t.Fatalf("Wait: got exit %d, want 1 after cleanup failure", code)
	}
}

// Cleanup is idempotent: the first call decides the exit code.
func TestCleanupIdempotent(t *testing.T) {
	fired := make(chan struct{})
	c := NewWithSignal(2*time.Second, fired)
	calls := 0
	c.Defer(func(context.Context) error { calls++; return errors.New("boom") })
	close(fired)
	if code := c.Cleanup(); code != 1 {
		t.Fatalf("first Cleanup: got %d, want 1", code)
	}
	if code := c.Cleanup(); code != 0 {
		t.Fatalf("second Cleanup: got %d, want 0 (idempotent)", code)
	}
	if calls != 1 {
		t.Fatalf("cleanups ran %d times, want 1", calls)
	}
}

// Cleanups receive a context bounded by the drain window: a cleanup that
// waits for it observes deadline expiry inside the cleanup itself.
func TestCleanupContextBounded(t *testing.T) {
	fired := make(chan struct{})
	c := NewWithSignal(50*time.Millisecond, fired)
	errCh := make(chan error, 1)
	c.Defer(func(ctx context.Context) error {
		<-ctx.Done() // simulate work exceeding the window
		errCh <- ctx.Err()
		return nil
	})
	close(fired)
	_ = c.Cleanup()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("cleanup ctx err: %v, want DeadlineExceeded", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cleanup context never expired")
	}
}
