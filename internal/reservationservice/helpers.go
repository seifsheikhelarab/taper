package reservationservice

import (
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

var errAlreadyProcessed = errors.New("idempotency key already processed")

func pgtypeTimestamptz(t time.Time) pgtype.Timestamptz {
	var ts pgtype.Timestamptz
	_ = ts.Scan(t)
	return ts
}
