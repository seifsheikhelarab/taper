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

// Idle eviction (spec #52, US12): churning more tenants than the floor
// bounds the map; the newest tenants keep their buckets.
func TestIdleEvictionBoundsMap(t *testing.T) {
	l := NewLimited(10, 5, 3)
	for i := 0; i < 10; i++ {
		if !l.Take(string(rune('a'+i))).Allowed {
			t.Fatalf("tenant %d: fresh bucket should pass", i)
		}
	}
	if got := l.Len(); got != 3 {
		t.Fatalf("buckets = %d, want 3 (floor)", got)
	}
	// The most recent tenants survived; the earliest were evicted.
	for _, keep := range []string{"h", "i", "j"} {
		if !l.Take(keep).Allowed {
			t.Fatalf("tenant %q bucket was evicted, want retained", keep)
		}
	}
}

// Eviction never touches an active tenant: refresh t1 between churning
// arrivals and its bucket (with its accumulated refill state) stays.
func TestEvictionSparesActiveTenants(t *testing.T) {
	l := NewLimited(1, 1, 2)
	if !l.Take("t1").Allowed {
		t.Fatal("t1 first take should pass")
	}
	if l.Take("t1").Allowed {
		t.Fatal("t1 second take should be limited (bucket empty)")
	}
	// Churn arrivals: evictions must pick idle buckets, and t1's entry is
	// refreshed on each Take so it is never the LRU victim.
	for i := 0; i < 20; i++ {
		l.Take(string(rune('a'+i)))
		l.Take("t1") // refresh + still limited (bucket drains at 1/s)
	}
	if got := l.Len(); got != 2 {
		t.Fatalf("buckets = %d, want 2", got)
	}
}

// Unbounded limiter (New) never evicts, matching pre-T5 behavior.
func TestUnboundedNeverEvicts(t *testing.T) {
	l := New(10, 5)
	for i := 0; i < 50; i++ {
		l.Take(string(rune('a' + i)))
	}
	if got := l.Len(); got != 50 {
		t.Fatalf("buckets = %d, want 50 (no floor)", got)
	}
}
