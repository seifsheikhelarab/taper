// Package idemprune implements the retention owner for
// processed_idempotency_keys (spec #52, B3 / US18): batched, transactional
// deletes of idempotency records older than the retention window, guarded
// by a per-database advisory lock so two workers never sweep concurrently.
// It deliberately mirrors pkg/outboxprune (ADR-0002's approach) — same
// batch/lock/interval shape, different table.
//
// Retention default: 7 days. Idempotency records only need to outlive the
// retry horizon of clients and the saga resume loop (minutes to hours);
// a week covers multi-day client retries with margin.
package idemprune

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Config tunes the sweeping worker.
type Config struct {
	// Interval between sweep passes. Defaults to 1h.
	Interval time.Duration
	// Retention: rows older than this are deleted. Defaults to 168h (7d).
	Retention time.Duration
	// BatchSize caps rows deleted per pass. Defaults to 1000.
	BatchSize int
}

func (c Config) withDefaults() Config {
	if c.Interval <= 0 {
		c.Interval = time.Hour
	}
	if c.Retention <= 0 {
		c.Retention = 7 * 24 * time.Hour
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 1000
	}
	return c
}

// Worker prunes processed_idempotency_keys on a schedule. Safe to run as
// one instance per database; the advisory lock prevents overlap.
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

// Run blocks, sweeping on the configured interval until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.Interval)
	defer ticker.Stop()
	// Sweep once at startup, then on every tick.
	w.sweepOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.sweepOnce(ctx)
		}
	}
}

func (w *Worker) sweepOnce(ctx context.Context) {
	deleted, err := w.PruneBatch(ctx)
	if err != nil {
		w.log("idemprune: pass failed: %v", err)
		return
	}
	if deleted > 0 {
		w.log("idemprune: deleted %d rows", deleted)
	}
}

// PruneBatch deletes one batch of expired idempotency records. Exported for
// unit testing; Run calls it on the schedule. Returns rows deleted (0 when
// another worker holds the advisory lock).
func (w *Worker) PruneBatch(ctx context.Context) (int64, error) {
	cfg := w.cfg
	cutoff := time.Now().Add(-cfg.Retention)

	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// 64-bit advisory lock; distinct from outboxprune's so the two
	// retention owners never serialize each other.
	const lockID = 0x74617065725F6964 // "taper_id"
	var locked bool
	if err := tx.QueryRow(ctx, "SELECT pg_try_advisory_xact_lock($1)", lockID).Scan(&locked); err != nil {
		return 0, err
	}
	if !locked {
		return 0, nil // another worker is sweeping; skip this pass
	}
	tag, err := tx.Exec(ctx, `
		DELETE FROM processed_idempotency_keys
		WHERE (tenant_id, idempotency_key) IN (
			SELECT tenant_id, idempotency_key
			FROM processed_idempotency_keys
			WHERE created_at < $1
			ORDER BY created_at
			LIMIT $2
		)`, cutoff, cfg.BatchSize)
	if err != nil {
		return 0, err
	}
	deleted := tag.RowsAffected()
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return deleted, nil
}
