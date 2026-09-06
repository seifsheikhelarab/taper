-- Channelsync service: availability projection and operational alerts.
-- Projection rows are keyed by the stream's composite partition key
-- (tenant:sku:warehouse); idempotency keys dedupe redelivered events.
CREATE TABLE availability_projection (
    tenant_id UUID NOT NULL,
    sku_id VARCHAR(255) NOT NULL,
    warehouse_id VARCHAR(255) NOT NULL,
    available_qty INT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, sku_id, warehouse_id)
);

CREATE TABLE alerts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL,
    sku_id VARCHAR(255) NOT NULL,
    warehouse_id VARCHAR(255) NOT NULL,
    delta INT NOT NULL,
    reason VARCHAR(255) NOT NULL,
    source VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE processed_idempotency_keys (
    tenant_id UUID NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL,
    payload_hash VARCHAR(255) NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, idempotency_key)
);

CREATE OR REPLACE FUNCTION app_current_tenant() RETURNS UUID LANGUAGE sql STABLE AS
$$ SELECT NULLIF(current_setting('app.current_tenant_id', true), '')::uuid $$;

ALTER TABLE availability_projection ENABLE ROW LEVEL SECURITY;
ALTER TABLE alerts ENABLE ROW LEVEL SECURITY;
ALTER TABLE processed_idempotency_keys ENABLE ROW LEVEL SECURITY;

CREATE POLICY availability_projection_tenant_isolation_policy ON availability_projection
    USING (tenant_id = app_current_tenant());

CREATE POLICY alerts_tenant_isolation_policy ON alerts
    USING (tenant_id = app_current_tenant());

CREATE POLICY processed_idempotency_keys_tenant_isolation_policy ON processed_idempotency_keys
    USING (tenant_id = app_current_tenant());

GRANT ALL ON ALL TABLES IN SCHEMA public TO taper_app;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO taper_app;
GRANT ALL ON ALL TABLES IN SCHEMA public TO taper_sweeper;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO taper_sweeper;
