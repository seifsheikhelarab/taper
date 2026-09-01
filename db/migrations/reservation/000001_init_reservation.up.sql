CREATE TABLE reservations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL,
    order_id VARCHAR(255) NOT NULL,
    sku_id VARCHAR(255) NOT NULL,
    warehouse_id VARCHAR(255) NOT NULL,
    quantity INT NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'ACTIVE',
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_reservations_ttl ON reservations(expires_at, status) WHERE status = 'ACTIVE';

CREATE TABLE outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    aggregate_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE processed_idempotency_keys (
    tenant_id UUID NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL,
    payload_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, idempotency_key)
);

ALTER TABLE reservations ENABLE ROW LEVEL SECURITY;
CREATE POLICY reservations_tenant_isolation_policy ON reservations
    USING (tenant_id = current_setting('app.current_tenant_id', true)::uuid);

ALTER TABLE outbox ENABLE ROW LEVEL SECURITY;
CREATE POLICY outbox_tenant_isolation_policy ON outbox
    USING (tenant_id = current_setting('app.current_tenant_id', true)::uuid);

ALTER TABLE processed_idempotency_keys ENABLE ROW LEVEL SECURITY;
CREATE POLICY processed_idempotency_keys_tenant_isolation_policy ON processed_idempotency_keys
    USING (tenant_id = current_setting('app.current_tenant_id', true)::uuid);
