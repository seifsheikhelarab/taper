package database

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
)

// ParseUUID converts a string tenant/SKU identifier into a pgtype.UUID.
func ParseUUID(s string) (pgtype.UUID, error) {
	var u pgtype.UUID
	if err := u.Scan(s); err != nil {
		return pgtype.UUID{}, fmt.Errorf("invalid UUID %q: %w", s, err)
	}
	return u, nil
}
