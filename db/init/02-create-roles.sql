-- Service roles. RLS only enforces tenant isolation against non-superusers.
CREATE ROLE taper_app LOGIN PASSWORD 'taperapp';
CREATE ROLE taper_sweeper LOGIN PASSWORD 'tapersweeper' BYPASSRLS;

-- Debezium CDC role: needs logical replication on every service database.
-- Contract (enforced by the service migrations): taper_cdc gets SELECT on
-- public.outbox, and the outbox RLS policy is scoped to taper_app so CDC
-- snapshots see every tenant's rows. Without both, connectors report RUNNING
-- while streaming nothing.
CREATE ROLE taper_cdc LOGIN PASSWORD 'tapercdc' REPLICATION;
GRANT CONNECT ON DATABASE taper_db, reservation_db, order_db TO taper_cdc;