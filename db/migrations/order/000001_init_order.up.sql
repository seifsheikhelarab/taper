CREATE TABLE orders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL,
    order_id VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    total_amount BIGINT NOT NULL DEFAULT 0,
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    transaction_id VARCHAR(255) NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, order_id)
);

CREATE TABLE saga_instances (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL,
    order_id VARCHAR(255) NOT NULL,
    state VARCHAR(50) NOT NULL,
    step INT NOT NULL DEFAULT 0,
    idempotency_key VARCHAR(255) NOT NULL,
    error VARCHAR(1024) NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, order_id)
);

CREATE INDEX idx_saga_instances_resume ON saga_instances(state) WHERE state IN ('PENDING_PAYMENT', 'RESERVED');

CREATE TABLE order_lines (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL,
    order_id VARCHAR(255) NOT NULL,
    sku_id VARCHAR(255) NOT NULL,
    warehouse_id VARCHAR(255) NOT NULL,
    quantity INT NOT NULL,
    unit_price BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, order_id, sku_id, warehouse_id)
);

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

CREATE OR REPLACE FUNCTION app_current_tenant() RETURNS UUID LANGUAGE sql STABLE AS
$$ SELECT NULLIF(current_setting('app.current_tenant_id', true), '')::uuid $$;

ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
ALTER TABLE saga_instances ENABLE ROW LEVEL SECURITY;
ALTER TABLE order_lines ENABLE ROW LEVEL SECURITY;
ALTER TABLE outbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE processed_idempotency_keys ENABLE ROW LEVEL SECURITY;

CREATE POLICY orders_tenant_isolation_policy ON orders
    USING (tenant_id = app_current_tenant());

CREATE POLICY saga_instances_tenant_isolation_policy ON saga_instances
    USING (tenant_id = app_current_tenant());

CREATE POLICY order_lines_tenant_isolation_policy ON order_lines
    USING (tenant_id = app_current_tenant());

CREATE POLICY outbox_tenant_isolation_policy ON outbox
    USING (tenant_id = app_current_tenant());

CREATE POLICY processed_idempotency_keys_tenant_isolation_policy ON processed_idempotency_keys
    USING (tenant_id = app_current_tenant());
