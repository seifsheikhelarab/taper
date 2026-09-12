-- Reverse of 000004_indexes_constraints.up.sql.
DROP INDEX IF EXISTS idx_stock_events_reconcile;

ALTER TABLE stock_levels DROP CONSTRAINT IF EXISTS stock_levels_allocated_nonneg;
ALTER TABLE stock_levels DROP CONSTRAINT IF EXISTS stock_levels_reserved_nonneg;
ALTER TABLE stock_levels DROP CONSTRAINT IF EXISTS stock_levels_available_nonneg;
