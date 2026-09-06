-- Service and cross-tenant maintenance access (000001 grants nothing;
-- other service migrations grant these). The sweeper role has BYPASSRLS
-- (db/init/02-create-roles.sql) for reconciliation/pruning.
GRANT ALL ON ALL TABLES IN SCHEMA public TO taper_app;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO taper_app;
GRANT ALL ON ALL TABLES IN SCHEMA public TO taper_sweeper;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO taper_sweeper;
