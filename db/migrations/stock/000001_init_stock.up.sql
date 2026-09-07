CREATE TABLE stock_levels (
    tenant_id UUID NOT NULL,
    sku_id VARCHAR(255) NOT NULL,
    warehouse_id VARCHAR(255) NOT NULL,
    available_qty INT NOT NULL DEFAULT 0,
    reserved_qty INT NOT NULL DEFAULT 0,
    allocated_qty INT NOT NULL DEFAULT 0,
    is_locked_for_audit BOOLEAN NOT NULL DEFAULT false,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, sku_id, warehouse_id)
);

CREATE TABLE stock_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL,
    sku_id VARCHAR(255) NOT NULL,
    warehouse_id VARCHAR(255) NOT NULL,
    delta INT NOT NULL,
    reason VARCHAR(255) NOT NULL,
    source VARCHAR(255) NOT NULL,
    actor_id VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
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

ALTER TABLE stock_levels ENABLE ROW LEVEL SECURITY;
ALTER TABLE stock_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE outbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE processed_idempotency_keys ENABLE ROW LEVEL SECURITY;

CREATE POLICY stock_levels_tenant_isolation_policy ON stock_levels
    USING (tenant_id = app_current_tenant());

CREATE POLICY stock_events_tenant_isolation_policy ON stock_events
    USING (tenant_id = app_current_tenant());

-- The outbox policy is scoped to taper_app only, and Debezium snapshots the
-- outbox as taper_cdc: RLS is default-deny for roles matching no policy, so
-- CDC needs its own explicit permissive policy (OR'd with the tenant policy)
-- to read every tenant's rows without a tenant context.
CREATE POLICY outbox_tenant_isolation_policy ON outbox
    TO taper_app
    USING (tenant_id = app_current_tenant());

CREATE POLICY outbox_cdc_read_policy ON outbox
    TO taper_cdc
    USING (true);

GRANT SELECT ON outbox TO taper_cdc;

CREATE POLICY processed_idempotency_keys_tenant_isolation_policy ON processed_idempotency_keys
    USING (tenant_id = app_current_tenant());