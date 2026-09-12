package observability

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// ReadyHandler reports 503 when any registered dependency fails, 200 when
// all pass, and honours SetDegraded for planned maintenance.
func TestReadyHandler(t *testing.T) {
	p, err := Setup(context.Background(), Config{ServiceName: "test"})
	if err != nil {
		t.Fatal(err)
	}

	get := func(h http.Handler) int {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", "/readyz", nil))
		return rec.Code
	}

	// No checks registered: ready.
	if code := get(p.ReadyHandler()); code != http.StatusOK {
		t.Fatalf("no checks: %d, want 200", code)
	}

	ok := func(context.Context) error { return nil }
	fail := func(context.Context) error { return errors.New("down") }

	p.RegisterReadiness("good", ok)
	if code := get(p.ReadyHandler()); code != http.StatusOK {
		t.Fatalf("good check: %d, want 200", code)
	}

	p.RegisterReadiness("bad", fail)
	if code := get(p.ReadyHandler()); code != http.StatusServiceUnavailable {
		t.Fatalf("bad check: %d, want 503", code)
	}

	// Degraded flips readiness regardless of dependency state...
	p.SetDegraded(true)
	p.RegisterReadiness("bad", ok)
	if code := get(p.ReadyHandler()); code != http.StatusServiceUnavailable {
		t.Fatalf("degraded: %d, want 503", code)
	}
	// ...and back.
	p.SetDegraded(false)
	if code := get(p.ReadyHandler()); code != http.StatusOK {
		t.Fatalf("undegraded: %d, want 200", code)
	}
}

// LiveHandler is static 200 liveness: independent of every dependency.
func TestLiveHandler(t *testing.T) {
	p, err := Setup(context.Background(), Config{ServiceName: "test"})
	if err != nil {
		t.Fatal(err)
	}
	p.RegisterReadiness("bad", func(context.Context) error { return errors.New("down") })
	p.SetDegraded(true)

	rec := httptest.NewRecorder()
	p.LiveHandler().ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("liveness while degraded: %d, want 200", rec.Code)
	}
}
