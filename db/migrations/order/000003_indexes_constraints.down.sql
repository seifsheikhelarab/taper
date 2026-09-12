-- Reverse of 000003_indexes_constraints.up.sql.
ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_total_amount_nonneg;
ALTER TABLE order_lines DROP CONSTRAINT IF EXISTS order_lines_unit_price_nonneg;
ALTER TABLE order_lines DROP CONSTRAINT IF EXISTS order_lines_quantity_positive;
ALTER TABLE saga_instances DROP CONSTRAINT IF EXISTS saga_instances_state_in_enum;
ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_status_in_enum;

DROP INDEX IF EXISTS idx_order_lines_order_id;
DROP INDEX IF EXISTS idx_saga_instances_order_id;
DROP INDEX IF EXISTS idx_orders_order_id;
