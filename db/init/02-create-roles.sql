-- Service roles. RLS only enforces tenant isolation against non-superusers.
CREATE ROLE taper_app LOGIN PASSWORD 'taperapp';
CREATE ROLE taper_sweeper LOGIN PASSWORD 'tapersweeper' BYPASSRLS;

-- Debezium CDC role: needs logical replication on every service database.
CREATE ROLE taper_cdc LOGIN PASSWORD 'tapercdc' REPLICATION;
GRANT CONNECT ON DATABASE taper_db, reservation_db, order_db TO taper_cdc;