-- Spec #52 (B3): lookup indexes on the per-order hot paths (gateway reads
-- by order_id; saga resume and replay scan saga_instances by order_id).
-- The (tenant_id, order_id) uniqueness exists; these serve order_id-only
-- lookups from the sweeper/fulfillment surfaces.
CREATE INDEX idx_orders_order_id ON orders (order_id);
CREATE INDEX idx_saga_instances_order_id ON saga_instances (order_id);
CREATE INDEX idx_order_lines_order_id ON order_lines (order_id);

-- CHECK constraints mirroring internal/orderservice/domain.go: the durable
-- saga state machine and the order status mirror pinned to their enums.
ALTER TABLE orders ADD CONSTRAINT orders_status_in_enum
    CHECK (status IN ('PENDING', 'CONFIRMED', 'FULFILLED', 'COMPENSATED', 'FAILED'));
ALTER TABLE saga_instances ADD CONSTRAINT saga_instances_state_in_enum
    CHECK (state IN ('PENDING_PAYMENT', 'RESERVED', 'ALLOCATED', 'CONFIRMED', 'COMPENSATED', 'FAILED'));

-- Value-level sanity on money and quantities.
ALTER TABLE order_lines ADD CONSTRAINT order_lines_quantity_positive
    CHECK (quantity > 0);
ALTER TABLE order_lines ADD CONSTRAINT order_lines_unit_price_nonneg
    CHECK (unit_price >= 0);
ALTER TABLE orders ADD CONSTRAINT orders_total_amount_nonneg
    CHECK (total_amount >= 0);
