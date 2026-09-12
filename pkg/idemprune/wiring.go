package idemprune

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// StartFromEnv starts the worker in a background goroutine when
// IDEMPOTENCY_PRUNE_ENABLED is truthy (spec #52, B3: same env-gated shape
// as the outbox prune worker). Interval and retention are tunable via
// IDEMPOTENCY_PRUNE_INTERVAL and IDEMPOTENCY_PRUNE_RETENTION; invalid
// values fall back to the defaults with a warning rather than failing a
// maintenance toggle.
func StartFromEnv(ctx context.Context, pool *pgxpool.Pool, logf func(format string, args ...any)) {
	switch os.Getenv("IDEMPOTENCY_PRUNE_ENABLED") {
	case "1", "true", "TRUE", "True", "yes", "YES", "Yes":
	default:
		return
	}
	if logf == nil {
		logf = log.Printf
	}
	cfg := Config{
		Interval:  envDuration("IDEMPOTENCY_PRUNE_INTERVAL", 0, logf),
		Retention: envDuration("IDEMPOTENCY_PRUNE_RETENTION", 0, logf),
	}
	w := New(pool, cfg, logf)
	go w.Run(ctx)
}

// envDuration reads a duration env with a fallback; 0 falls through to the
// Config default.
func envDuration(key string, def time.Duration, logf func(format string, args ...any)) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	if d, err := time.ParseDuration(v); err == nil && d > 0 {
		return d
	}
	logf("idemprune: invalid %s=%q, using default", key, v)
	return def
}
