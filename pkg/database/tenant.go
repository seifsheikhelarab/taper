package database

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type tenantKey struct{}

var (
	ErrTenantIDMissing = errors.New("tenant ID missing from context")
)

// WithTenantID embeds tenant ID into context.
func WithTenantID(ctx context.Context, tenantID string) context.Context {
	return context.WithValue(ctx, tenantKey{}, tenantID)
}

// TenantIDFromContext extracts tenant ID from context.
func TenantIDFromContext(ctx context.Context) (string, error) {
	tenantID, ok := ctx.Value(tenantKey{}).(string)
	if !ok || tenantID == "" {
		return "", ErrTenantIDMissing
	}
	return tenantID, nil
}

// SetTenantRLS sets the PostgreSQL RLS tenant configuration setting for the current transaction scope.
func SetTenantRLS(ctx context.Context, tx pgx.Tx, tenantID string) error {
	_, err := tx.Exec(ctx, "SELECT set_config('app.current_tenant_id', $1, true)", tenantID)
	if err != nil {
		return fmt.Errorf("failed to set tenant RLS context: %w", err)
	}
	return nil
}

// ExecTxWithTenant executes callback function within a PostgreSQL transaction with tenant RLS set.
func ExecTxWithTenant(ctx context.Context, pool *pgxpool.Pool, fn func(tx pgx.Tx) error) error {
	tenantID, err := TenantIDFromContext(ctx)
	if err != nil {
		return err
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if err := SetTenantRLS(ctx, tx, tenantID); err != nil {
		return err
	}

	if err := fn(tx); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
