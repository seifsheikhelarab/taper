package outboxprune

import (
	"context"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// StartFromEnv starts the worker in a background goroutine when
// OUTBOX_PRUNE_ENABLED is truthy ("1", "true", "yes", case-insensitive).
// Interval/retention/batch size are tunable via OUTBOX_PRUNE_INTERVAL,
// OUTBOX_PRUNE_RETENTION, OUTBOX_PRUNE_BATCH_SIZE. Returns the Worker and a
// cancel func (no-op when disabled).
func StartFromEnv(ctx context.Context, pool *pgxpool.Pool, log func(format string, args ...any)) (*Worker, func()) {
	if !enabled() {
		return nil, func() {}
	}
	cfg := Config{
		Interval:  durationEnv("OUTBOX_PRUNE_INTERVAL", 0),
		Retention: durationEnv("OUTBOX_PRUNE_RETENTION", 0),
		BatchSize: intEnv("OUTBOX_PRUNE_BATCH_SIZE", 0),
	}
	w := New(pool, cfg, log)
	go w.Run(ctx)
	return w, func() {}
}

func enabled() bool {
	switch os.Getenv("OUTBOX_PRUNE_ENABLED") {
	case "1", "true", "TRUE", "True", "yes", "YES", "Yes":
		return true
	default:
		return false
	}
}

func durationEnv(name string, def time.Duration) time.Duration {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return def
	}
	return d
}

func intEnv(name string, def int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return n
}
