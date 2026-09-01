package reservationservice

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	resdb "github.com/seifsheikhelarab/taper/gen/go/db/reservation"
	resv1 "github.com/seifsheikhelarab/taper/gen/go/reservation/v1"
	stockv1 "github.com/seifsheikhelarab/taper/gen/go/stock/v1"
	"github.com/seifsheikhelarab/taper/pkg/database"
)

// Server implements reservation.v1.ReservationService.
type Server struct {
	resv1.UnimplementedReservationServiceServer
	pool       *pgxpool.Pool
	stock      stockv1.StockServiceClient
	defaultTTL time.Duration
}

func NewServer(pool *pgxpool.Pool, stock stockv1.StockServiceClient) *Server {
	return &Server{
		pool:       pool,
		stock:      stock,
		defaultTTL: 15 * time.Minute,
	}
}

// Reserve holds stock for all requested lines (all-or-nothing) and records the reservation.
func (s *Server) Reserve(ctx context.Context, req *resv1.ReserveRequest) (*resv1.ReserveResponse, error) {
	tenantUUID, err := database.ParseUUID(req.GetTenantId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if len(req.GetItems()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "no items provided")
	}

	ttl := s.defaultTTL
	if req.GetTtlSeconds() > 0 {
		ttl = time.Duration(req.GetTtlSeconds()) * time.Second
	}

	stockLines := make([]*stockv1.StockLine, 0, len(req.GetItems()))
	for _, it := range req.GetItems() {
		stockLines = append(stockLines, &stockv1.StockLine{
			SkuId:       it.GetSkuId(),
			WarehouseId: it.GetWarehouseId(),
			Quantity:    it.GetQuantity(),
		})
	}

	// All-or-nothing stock hold.
	stockResp, err := s.stock.ReserveStock(ctx, &stockv1.ReserveStockRequest{
		TenantId:       req.GetTenantId(),
		OrderId:        req.GetOrderId(),
		IdempotencyKey: req.GetIdempotencyKey(),
		Lines:          stockLines,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "reserve stock: %v", err)
	}
	if !stockResp.GetSuccess() {
		return &resv1.ReserveResponse{Success: false, FailedSkuIds: stockResp.GetFailedSkuIds()}, nil
	}

	expiresAt := time.Now().Add(ttl)
	reservationID, err := s.insertReservations(ctx, tenantUUID, req, expiresAt)
	if err != nil {
		// Compensate: release the stock hold taken above.
		_, _ = s.stock.ReleaseStock(context.Background(), &stockv1.ReleaseStockRequest{
			TenantId: req.GetTenantId(),
			OrderId:  req.GetOrderId(),
			Reason:   "reserve_failure",
			Lines:    stockLines,
		})
		return nil, status.Errorf(codes.Internal, "persist reservation: %v", err)
	}

	return &resv1.ReserveResponse{
		Success:       true,
		ReservationId: reservationID,
		ExpiresAtUnix: expiresAt.Unix(),
	}, nil
}

func (s *Server) insertReservations(ctx context.Context, tenantUUID pgtype.UUID, req *resv1.ReserveRequest, expiresAt time.Time) (string, error) {
	var firstID string
	err := database.ExecTxWithTenant(ctx, s.pool, func(tx pgx.Tx) error {
		q := resdb.New(tx)
		if req.GetIdempotencyKey() != "" {
			_, err := q.CheckAndInsertIdempotencyKey(ctx, resdb.CheckAndInsertIdempotencyKeyParams{
				TenantID:       tenantUUID,
				IdempotencyKey: req.GetIdempotencyKey(),
				PayloadHash:    hashPayload(req),
			})
			if err == pgx.ErrNoRows {
				return errAlreadyReserved
			}
			if err != nil {
				return err
			}
		}
		for _, it := range req.GetItems() {
			row, err := q.InsertReservation(ctx, resdb.InsertReservationParams{
				TenantID:    tenantUUID,
				OrderID:     req.GetOrderId(),
				SkuID:       it.GetSkuId(),
				WarehouseID: it.GetWarehouseId(),
				Quantity:    it.GetQuantity(),
				Status:      "ACTIVE",
				ExpiresAt:   pgtypeTimestamptz(expiresAt),
			})
			if err != nil {
				return err
			}
			if firstID == "" {
				firstID = row.ID.String()
			}
			if _, err := q.InsertOutboxEvent(ctx, resdb.InsertOutboxEventParams{
				TenantID:      tenantUUID,
				AggregateType: "reservation",
				AggregateID:   req.GetOrderId(),
				EventType:     "reservation.created",
				Payload:       []byte(`{"order_id":"` + req.GetOrderId() + `","sku_id":"` + it.GetSkuId() + `"}`),
			}); err != nil {
				return err
			}
		}
		return nil
	})
	return firstID, err
}

// Release cancels an active reservation and returns stock to available.
func (s *Server) Release(ctx context.Context, req *resv1.ReleaseRequest) (*resv1.ReleaseResponse, error) {
	tenantUUID, err := database.ParseUUID(req.GetTenantId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	var released []string
	err = database.ExecTxWithTenant(ctx, s.pool, func(tx pgx.Tx) error {
		q := resdb.New(tx)
		if req.GetIdempotencyKey() != "" {
			_, err := q.CheckAndInsertIdempotencyKey(ctx, resdb.CheckAndInsertIdempotencyKeyParams{
				TenantID:       tenantUUID,
				IdempotencyKey: req.GetIdempotencyKey(),
				PayloadHash:    hashPayload(req),
			})
			if err == pgx.ErrNoRows {
				return errAlreadyReleased
			}
			if err != nil {
				return err
			}
		}

		rows, err := q.GetActiveReservationsByOrder(ctx, req.GetOrderId())
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil
		}

		var stockLines []*stockv1.StockLine
		for _, r := range rows {
			stockLines = append(stockLines, &stockv1.StockLine{
				SkuId:       r.SkuID,
				WarehouseId: r.WarehouseID,
				Quantity:    r.Quantity,
			})
		}

		// Return stock to available.
		stockResp, err := s.stock.ReleaseStock(ctx, &stockv1.ReleaseStockRequest{
			TenantId: req.GetTenantId(),
			OrderId:  req.GetOrderId(),
			Reason:   req.GetReason(),
			Lines:    stockLines,
		})
		if err != nil {
			return err
		}
		released = stockResp.GetReleasedSkuIds()

		for _, r := range rows {
			if _, err := q.UpdateReservationStatus(ctx, resdb.UpdateReservationStatusParams{
				ID:     r.ID,
				Status: "RELEASED",
			}); err != nil {
				return err
			}
			if _, err := q.InsertOutboxEvent(ctx, resdb.InsertOutboxEventParams{
				TenantID:      tenantUUID,
				AggregateType: "reservation",
				AggregateID:   req.GetOrderId(),
				EventType:     "reservation.released",
				Payload:       []byte(`{"order_id":"` + req.GetOrderId() + `"}`),
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, errAlreadyReleased) {
			return &resv1.ReleaseResponse{Success: true}, nil
		}
		return nil, status.Errorf(codes.Internal, "release: %v", err)
	}
	return &resv1.ReleaseResponse{Success: true, ReleasedSkuIds: released}, nil
}
