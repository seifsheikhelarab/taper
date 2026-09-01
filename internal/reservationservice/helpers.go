package reservationservice

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/proto"
)

var (
	errAlreadyReserved = errors.New("idempotency key already processed")
	errAlreadyReleased = errors.New("idempotency key already processed")
)

func hashPayload(msg proto.Message) string {
	b, _ := proto.Marshal(msg)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func pgtypeTimestamptz(t time.Time) pgtype.Timestamptz {
	var ts pgtype.Timestamptz
	_ = ts.Scan(t)
	return ts
}
