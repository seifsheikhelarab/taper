DROP TABLE IF EXISTS processed_idempotency_keys;
DROP TABLE IF EXISTS outbox;
DROP TABLE IF EXISTS order_lines;
DROP TABLE IF EXISTS saga_instances;
DROP TABLE IF EXISTS orders;
DROP FUNCTION IF EXISTS app_current_tenant();
