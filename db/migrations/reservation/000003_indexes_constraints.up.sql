-- Spec #52 (B3): CHECK constraints mirroring the reservation status
-- constants in internal/reservationservice/domain.go (US5: centralized
-- instead of scattered literals) and positive line quantities.
ALTER TABLE reservations ADD CONSTRAINT reservations_status_in_enum
    CHECK (status IN ('ACTIVE', 'ALLOCATED', 'RELEASED', 'EXPIRED'));
ALTER TABLE reservations ADD CONSTRAINT reservations_quantity_positive
    CHECK (quantity > 0);
