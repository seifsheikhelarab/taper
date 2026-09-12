package reservationservice

import (
	"context"
	"github.com/seifsheikhelarab/taper/pkg/database"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	resdb "github.com/seifsheikhelarab/taper/gen/go/db/reservation"
	stockv1 "github.com/seifsheikhelarab/taper/gen/go/stock/v1"
)

// Sweeper periodically releases stock for expired reservations.
type Sweeper struct {
	pool     *pgxpool.Pool
	stock    stockv1.StockServiceClient
	interval time.Duration
	batch    int32
}

func NewSweeper(pool *pgxpool.Pool, stock stockv1.StockServiceClient) *Sweeper {
	return &Sweeper{
		pool:     pool,
		stock:    stock,
		interval: 30 * time.Second,
		batch:    100,
	}
}

func (s *Sweeper) Run(ctx context.Context) {
	for {
		s.sweepOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.interval):
		}
	}
}

func (s *Sweeper) sweepOnce(ctx context.Context) {
	expired, err := resdb.New(s.pool).GetExpiredReservations(ctx, resdb.GetExpiredReservationsParams{
		ExpiresAt: pgtypeTimestamptz(time.Now()),
		Limit:     s.batch,
	})
	if err != nil {
		log.Printf("sweeper: fetch expired: %v", err)
		return
	}

	for k, rows := range groupByOrder(expired) {
		release := releaseRequest(k.tenant, k.orderID, rows)
		// Use compensation key to avoid idempotency suppression by stock service.
		if _, err := s.stock.ReleaseStock(ctx, release); err != nil {
			log.Printf("sweeper: release stock order %s: %v", k.orderID, err)
			continue
		}
		for _, r := range rows {
			s.markExpired(ctx, r)
		}
	}
}

// groupKey identifies one release unit: a reservation batch is released
// per (tenant, order) in a single idempotently-keyed stock call.
type groupKey struct {
	tenant  string
	orderID string
}

// groupByOrder groups expired reservations by (tenant_id, order_id).
// Extracted for unit testing (spec #52, T7): the grouping drives which
// stock lines ride one release and which compensation key covers them.
func groupByOrder(rows []resdb.Reservation) map[groupKey][]resdb.Reservation {
	groups := map[groupKey][]resdb.Reservation{}
	for _, r := range rows {
		k := groupKey{tenant: r.TenantID.String(), orderID: r.OrderID}
		groups[k] = append(groups[k], r)
	}
	return groups
}

// releaseRequest builds the stock release for one (tenant, order) group:
// every line of the batch, with the compensation key so a sweeper replay
// is exactly-once at the stock service.
func releaseRequest(tenant, orderID string, rows []resdb.Reservation) *stockv1.ReleaseStockRequest {
	var stockLines []*stockv1.StockLine
	for _, r := range rows {
		stockLines = append(stockLines, &stockv1.StockLine{
			SkuId:       r.SkuID,
			WarehouseId: r.WarehouseID,
			Quantity:    r.Quantity,
		})
	}
	return &stockv1.ReleaseStockRequest{
		TenantId:       tenant,
		OrderId:        orderID,
		Reason:         "ttl_expiry",
		IdempotencyKey: database.CompensationKey(tenant, orderID),
		Lines:          stockLines,
	}
}

func (s *Sweeper) markExpired(ctx context.Context, r resdb.Reservation) {
	_, err := resdb.New(s.pool).UpdateReservationStatus(ctx, resdb.UpdateReservationStatusParams{
		ID:     r.ID,
		Status: StatusExpired,
	})
	if err != nil {
		log.Printf("sweeper: mark expired %s: %v", r.ID.String(), err)
		return
	}
	_, err = resdb.New(s.pool).InsertOutboxEvent(ctx, resdb.InsertOutboxEventParams{
		TenantID:      r.TenantID,
		AggregateType: "reservation",
		AggregateID:   partitionKey(r.TenantID, r.OrderID),
		EventType:     "reservation.expired",
		Payload:       []byte(`{"order_id":"` + r.OrderID + `","sku_id":"` + r.SkuID + `"}`),
		Traceparent:   database.TraceparentText(ctx),
	})
	if err != nil {
		log.Printf("sweeper: outbox expired %s: %v", r.ID.String(), err)
	}
}
