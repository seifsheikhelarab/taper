package outboxprune

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/seifsheikhelarab/taper/pkg/config"
)

// StartFromEnv starts the worker in a background goroutine when
// OUTBOX_PRUNE_ENABLED is truthy; otherwise it is a no-op. Interval is
// tunable via OUTBOX_PRUNE_INTERVAL.
func StartFromEnv(ctx context.Context, pool *pgxpool.Pool, log func(format string, args ...any)) {
	if !config.EnvBool("OUTBOX_PRUNE_ENABLED") {
		return
	}
	w := New(pool, Config{
		Interval: config.EnvDuration("OUTBOX_PRUNE_INTERVAL", 0),
	}, log)
	go w.Run(ctx)
}
