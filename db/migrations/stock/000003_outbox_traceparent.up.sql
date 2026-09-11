-- Phase 5 (spec #44): carry W3C trace context through Debezium.
-- The EventRouter promotes this column to a Kafka header via
-- table.fields.additional.placement, so consumers can join the trace
-- that produced the event. Nullable: events written without an active
-- span (e.g. sweepers) simply carry no trace context.
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS traceparent text;
