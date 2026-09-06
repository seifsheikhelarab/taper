package outboxprune

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/seifsheikhelarab/taper/pkg/config"
)

// StartFromEnv starts the worker in a background goroutine when
// OUTBOX_PRUNE_ENABLED is truthy. Interval/retention/batch size are tunable
// via OUTBOX_PRUNE_INTERVAL, OUTBOX_PRUNE_RETENTION, OUTBOX_PRUNE_BATCH_SIZE.
// Returns the Worker and a cancel func (no-op when disabled).
func StartFromEnv(ctx context.Context, pool *pgxpool.Pool, log func(format string, args ...any)) (*Worker, func()) {
	if !config.EnvBool("OUTBOX_PRUNE_ENABLED") {
		return nil, func() {}
	}
	cfg := Config{
		Interval:  config.EnvDuration("OUTBOX_PRUNE_INTERVAL", 0),
		Retention: config.EnvDuration("OUTBOX_PRUNE_RETENTION", 0),
		BatchSize: config.EnvInt("OUTBOX_PRUNE_BATCH_SIZE", 0),
	}
	w := New(pool, cfg, log)
	go w.Run(ctx)
	return w, func() {}
}
