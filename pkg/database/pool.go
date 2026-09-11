package database

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultMaxConns is the pool ceiling used when DB_POOL_MAX_CONNS is unset.
// pgx's default of max(4, numCPU) silently caps throughput under load
// (measured during the Phase 5 load runs: 8 conns x ~138ms/tx = ~58 rps with
// 5s queue times), so sizing is explicit here instead.
const DefaultMaxConns = 32

// OpenPool opens a pgx pool with explicit sizing. DB_POOL_MAX_CONNS
// overrides the ceiling per deployment.
func OpenPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse pool config: %w", err)
	}
	if v := os.Getenv("DB_POOL_MAX_CONNS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("DB_POOL_MAX_CONNS: %w", err)
		}
		cfg.MaxConns = int32(n)
	} else {
		cfg.MaxConns = DefaultMaxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open pool: %w", err)
	}
	return pool, nil
}
