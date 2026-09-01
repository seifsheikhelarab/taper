-- Service roles. RLS only enforces tenant isolation against non-superusers.
CREATE ROLE taper_app LOGIN PASSWORD 'taperapp';
CREATE ROLE taper_sweeper LOGIN PASSWORD 'tapersweeper' BYPASSRLS;