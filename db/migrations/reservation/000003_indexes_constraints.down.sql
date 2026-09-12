-- Reverse of 000003_indexes_constraints.up.sql.
ALTER TABLE reservations DROP CONSTRAINT IF EXISTS reservations_quantity_positive;
ALTER TABLE reservations DROP CONSTRAINT IF EXISTS reservations_status_in_enum;
