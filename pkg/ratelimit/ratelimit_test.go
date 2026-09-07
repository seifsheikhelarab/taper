package ratelimit

import (
	"testing"
	"time"
)

func TestBurstExhaustionAndRefill(t *testing.T) {
	l := New(10, 3) // 10/s refill, burst 3
	if !l.Take("t1").Allowed {
		t.Fatal("first take should pass")
	}
	if !l.Take("t1").Allowed {
		t.Fatal("second take should pass")
	}
	if !l.Take("t1").Allowed {
		t.Fatal("third take should pass")
	}
	d := l.Take("t1")
	if d.Allowed {
		t.Fatal("fourth take should be limited")
	}
	if d.RetryAfter <= 0 || d.RetryAfter > 200*time.Millisecond {
		t.Fatalf("retry after = %v, want ~100ms at 10/s", d.RetryAfter)
	}

	// Advance time past a full refill window.
	l.now = func() time.Time { return time.Now().Add(time.Second) }
	if !l.Take("t1").Allowed {
		t.Fatal("take after refill should pass")
	}
}

func TestTenantIsolation(t *testing.T) {
	l := New(1, 1)
	if !l.Take("t1").Allowed {
		t.Fatal("t1 first take should pass")
	}
	if l.Take("t1").Allowed {
		t.Fatal("t1 second take should be limited")
	}
	if !l.Take("t2").Allowed {
		t.Fatal("t2 must have its own bucket")
	}
}

func TestDisabledWhenRateOrBurstZero(t *testing.T) {
	for _, cfg := range [][2]float64{{0, 10}, {10, 0}, {0, 0}} {
		l := New(cfg[0], cfg[1])
		for i := 0; i < 100; i++ {
			if !l.Take("t").Allowed {
				t.Fatalf("rate=%v burst=%v: unlimited takes expected", cfg[0], cfg[1])
			}
		}
	}
}
