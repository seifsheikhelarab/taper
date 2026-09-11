package stockservice

import (
	"context"
	"encoding/json"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/proto"

	stockdb "github.com/seifsheikhelarab/taper/gen/go/db/stock"
	stockv1 "github.com/seifsheikhelarab/taper/gen/go/stock/v1"
	"github.com/seifsheikhelarab/taper/pkg/database"
)

// StockLocation identifies a unique stock position (tenant + SKU + warehouse).
type StockLocation struct {
	TenantID    pgtype.UUID
	SkuID       string
	WarehouseID string
}

// tryIdempotency reports whether the key was already processed (duplicate).
// A duplicate means the prior attempt committed, so the caller must return
// the cached response without mutating anything (US8 exact-once).
func tryIdempotency(ctx context.Context, q *stockdb.Queries, tenantID pgtype.UUID, key string, msg proto.Message) (bool, error) {
	if key == "" {
		return false, nil
	}
	_, err := q.CheckAndInsertIdempotencyKey(ctx, stockdb.CheckAndInsertIdempotencyKeyParams{
		TenantID:       tenantID,
		IdempotencyKey: key,
		PayloadHash:    database.PayloadHash(msg),
	})
	if err == pgx.ErrNoRows {
		return true, nil
	}
	return false, err
}

// stockLevelParams returns the common fields for stock level queries.
func stockLevelParams(loc StockLocation) stockdb.GetStockLevelForUpdateParams {
	return stockdb.GetStockLevelForUpdateParams{
		TenantID:    loc.TenantID,
		SkuID:       loc.SkuID,
		WarehouseID: loc.WarehouseID,
	}
}

// sortedLines orders lines deterministically to avoid cross-row deadlocks.
func sortedLines(in []*stockv1.StockLine) []*stockv1.StockLine {
	out := make([]*stockv1.StockLine, len(in))
	copy(out, in)
	sort.Slice(out, func(i, j int) bool {
		if out[i].SkuId != out[j].SkuId {
			return out[i].SkuId < out[j].SkuId
		}
		return out[i].WarehouseId < out[j].WarehouseId
	})
	return out
}

func marshalLineEvent(action, orderID string, l *stockv1.StockLine) []byte {
	b, _ := json.Marshal(map[string]any{
		"action":       action,
		"order_id":     orderID,
		"sku_id":       l.SkuId,
		"warehouse_id": l.WarehouseId,
		"quantity":     l.Quantity,
	})
	return b
}

func marshalAdjustEvent(req *stockv1.AdjustStockRequest, newAvailable int32) ([]byte, error) {
	b, err := json.Marshal(map[string]any{
		"sku_id":         req.GetSkuId(),
		"warehouse_id":   req.GetWarehouseId(),
		"quantity_delta": req.GetQuantityDelta(),
		// Resulting absolute so consumers (e.g. channel availability
		// projections) can upsert without replaying history.
		"available_qty": newAvailable,
		"reason":        req.GetReason(),
		"source":        req.GetSource(),
	})
	return b, err
}
