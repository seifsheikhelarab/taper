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

CREATE OR REPLACE FUNCTION app_current_tenant() RETURNS UUID LANGUAGE sql STABLE AS
$$ SELECT NULLIF(current_setting('app.current_tenant_id', true), '')::uuid $$;

ALTER TABLE reservations ENABLE ROW LEVEL SECURITY;
-- Service and cross-tenant maintenance access (other service migrations
-- grant these; reservation_db was missing them, so taper_app had zero
-- privileges on a fresh volume).
GRANT ALL ON ALL TABLES IN SCHEMA public TO taper_app;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO taper_app;
GRANT ALL ON ALL TABLES IN SCHEMA public TO taper_sweeper;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO taper_sweeper;

CREATE POLICY reservations_tenant_isolation_policy ON reservations
    USING (tenant_id = app_current_tenant());

ALTER TABLE outbox ENABLE ROW LEVEL SECURITY;
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

ALTER TABLE processed_idempotency_keys ENABLE ROW LEVEL SECURITY;
CREATE POLICY processed_idempotency_keys_tenant_isolation_policy ON processed_idempotency_keys
    USING (tenant_id = app_current_tenant());
