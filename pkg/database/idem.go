package database

import (
	"crypto/sha256"
	"encoding/hex"

	"google.golang.org/protobuf/proto"
)

// PayloadHash fingerprints a request for the idempotency ledger's
// payload_hash column. Services that store requests for replay (order,
// reservation, stock) share this so the convention stays one definition.
func PayloadHash(msg proto.Message) string {
	b, _ := proto.Marshal(msg)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// CompensationKey builds the shared "comp:" idempotency convention the order
// saga and the reservation service both use for compensation-release calls,
// so the stock service never mistakes them for duplicate reserves.
func CompensationKey(tenantID, orderID string) string {
	return "comp:" + tenantID + ":" + orderID
}