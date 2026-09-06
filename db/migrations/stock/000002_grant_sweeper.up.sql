-- Phase 3: cross-tenant maintenance access for the reconciliation worker
-- (BYPASSRLS already set on the role in db/init/02-create-roles.sql).
GRANT ALL ON ALL TABLES IN SCHEMA public TO taper_sweeper;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO taper_sweeper;
