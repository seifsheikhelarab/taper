-- No-oversell audit for load-test runs (spec #44, T4 / ADR-0001 invariant).
--
-- Run against taper_db after a load run (psql exits non-zero on error, and
-- zero violating rows is the pass condition a human or script asserts):
--
--   docker exec -i taper-postgres psql -U postgres -d taper_db \
--     -v on_error_stop=1 < scripts/audit-oversell.sql
--
-- The invariant (CONTEXT.md / ADR-0001): available + reserved + allocated
-- must never go negative. A negative bucket means more units left sellable
-- stock than ever existed — i.e. an oversell happened at some point during
-- the run, even if current state looks healthy.
--
-- Row-vs-reservations drift (reserved bucket vs ACTIVE/ALLOCATED
-- reservation quantities, all statuses live in reservation_db) is the
-- reconciliation worker's domain: ADR-0001's drift-locking audit already
-- halts the row on disagreement; this script does not duplicate it.

SELECT tenant_id, sku_id, warehouse_id,
       available_qty, reserved_qty, allocated_qty
FROM stock_levels
WHERE available_qty < 0 OR reserved_qty < 0 OR allocated_qty < 0;
