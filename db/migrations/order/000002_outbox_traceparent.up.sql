-- Phase 5 (spec #44): carry W3C trace context through Debezium. See
-- db/migrations/stock/000003_outbox_traceparent.up.sql for rationale.
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS traceparent text;
