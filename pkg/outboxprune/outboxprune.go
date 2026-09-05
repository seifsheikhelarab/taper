// Package outboxprune implements the ADR-0002 outbox retention worker:
// batched, transactional deletes of outbox rows older than the retention
// window, guarded by a per-database advisory lock so two workers never prune
// concurrently (CONTEXT.md: never concurrently).
package outboxprune

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Config tunes the pruning worker.
type Config struct {
	// Interval between prune passes. Defaults to 1h.
	Interval time.Duration
	// Retention: rows older than this are deleted. Defaults to 72h (3 days).
	Retention time.Duration
	// BatchSize caps rows deleted per pass. Defaults to 1000.
	BatchSize int
}

func (c Config) withDefaults() Config {
	if c.Interval <= 0 {
		c.Interval = time.Hour
	}
	if c.Retention <= 0 {
		c.Retention = 72 * time.Hour
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 1000
	}
	return c
}

// Worker prunes the outbox table on a schedule. Safe to run as one instance
// per database; the advisory lock prevents overlap across restarts.
type Worker struct {
	pool *pgxpool.Pool
	cfg  Config
	log  func(format string, args ...any)
}

// New creates a worker bound to a pool (use the BYPASSRLS sweeper role DSN:
// pruning is a cross-tenant maintenance task).
func New(pool *pgxpool.Pool, cfg Config, log func(format string, args ...any)) *Worker {
	if log == nil {
		log = func(string, ...any) {}
	}
	return &Worker{pool: pool, cfg: cfg.withDefaults(), log: log}
}

// Run blocks, pruning on the configured interval until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.Interval)
	defer ticker.Stop()
	// Prune once at startup, then on every tick.
	w.pruneOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.pruneOnce(ctx)
		}
	}
}

// pruneOnce executes a single guarded prune pass.
func (w *Worker) pruneOnce(ctx context.Context) {
	deleted, err := w.PruneBatch(ctx)
	if err != nil {
		w.log("outboxprune: pass failed: %v", err)
		return
	}
	if deleted > 0 {
		w.log("outboxprune: deleted %d rows", deleted)
	}
}

// PruneBatch deletes one batch of expired outbox rows. It is exported for
// unit testing; Run calls it on the schedule. Returns the number of rows
// deleted (0 when another worker holds the advisory lock).
func (w *Worker) PruneBatch(ctx context.Context) (int64, error) {
	cfg := w.cfg
	cutoff := time.Now().Add(-cfg.Retention)

	var deleted int64
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// 64-bit advisory lock; the id is arbitrary but stable per database.
	const lockID = 0x74617065725F6F62 // "taper_ob"
	var locked bool
	if err := tx.QueryRow(ctx, "SELECT pg_try_advisory_xact_lock($1)", lockID).Scan(&locked); err != nil {
		return 0, err
	}
	if !locked {
		return 0, nil // another worker is pruning; skip this pass
	}
	tag, err := tx.Exec(ctx, `
		DELETE FROM outbox
		WHERE id IN (
			SELECT id FROM outbox
			WHERE created_at < $1
			ORDER BY created_at
			LIMIT $2
		)`, cutoff, cfg.BatchSize)
	if err != nil {
		return 0, err
	}
	deleted = tag.RowsAffected()
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return deleted, nil
}
