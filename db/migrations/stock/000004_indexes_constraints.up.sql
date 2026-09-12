-- Spec #52 (B3 Data integrity & query): lookup index for the reconciliation
-- scan and CHECK constraints mirroring the domain invariants in CONTEXT.md.
--
-- The reconciliation worker (ADR-0001) replays stock_events per
-- (tenant_id, sku_id, warehouse_id); without this index every pass is a
-- sequential scan per location.
CREATE INDEX idx_stock_events_reconcile
    ON stock_events (tenant_id, sku_id, warehouse_id);

-- Impossible stock arithmetic is rejected at the value level (no write
-- path can silently corrupt the invariant the reconciliation scan exists
-- to catch). Quantities may not go negative; delta is a signed movement.
ALTER TABLE stock_levels ADD CONSTRAINT stock_levels_available_nonneg
    CHECK (available_qty >= 0);
ALTER TABLE stock_levels ADD CONSTRAINT stock_levels_reserved_nonneg
    CHECK (reserved_qty >= 0);
ALTER TABLE stock_levels ADD CONSTRAINT stock_levels_allocated_nonneg
    CHECK (allocated_qty >= 0);

-- stock_events.reason is deliberately NOT pinned to an enum: the
-- derivation contract in internal/stockservice/reconcile.go buckets known
-- reasons (adjust/external/reserve/release/allocate/fulfill) and treats
-- everything else as a plain available-delta, so operational reasons like
-- "ttl_expiry" or "reserve_failure" are legitimate. Only the write-side
-- non-negativity invariants are enforced here.
