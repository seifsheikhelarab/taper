package stockservice

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	stockdb "github.com/seifsheikhelarab/taper/gen/go/db/stock"
	stockv1 "github.com/seifsheikhelarab/taper/gen/go/stock/v1"
	"github.com/seifsheikhelarab/taper/pkg/database"
)

// Server implements stock.v1.StockService.
type Server struct {
	stockv1.UnimplementedStockServiceServer
	pool *pgxpool.Pool
}

func NewServer(pool *pgxpool.Pool) *Server {
	return &Server{pool: pool}
}

// AdjustStock changes available stock by a delta (external sync), clamping to 0.
func (s *Server) AdjustStock(ctx context.Context, req *stockv1.AdjustStockRequest) (*stockv1.AdjustStockResponse, error) {
	tenantUUID, err := database.ParseUUID(req.GetTenantId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ctx = database.WithTenantID(ctx, req.GetTenantId())
	loc := StockLocation{TenantID: tenantUUID, SkuID: req.GetSkuId(), WarehouseID: req.GetWarehouseId()}

	var resp *stockv1.AdjustStockResponse
	err = database.ExecTxWithTenant(ctx, s.pool, func(tx pgx.Tx) error {
		q := stockdb.New(tx)

		dup, err := tryIdempotency(ctx, q, tenantUUID, req.GetIdempotencyKey(), req)
		if err != nil {
			return err
		}
		if dup {
			level, err := q.GetStockLevelForUpdate(ctx, stockLevelParams(loc))
			if err != nil {
				return err
			}
			resp = &stockv1.AdjustStockResponse{
				Success:       true,
				AvailableQty:  level.AvailableQty,
				ReservedQty:   level.ReservedQty,
				AllocatedQty:  level.AllocatedQty,
				IsClampedZero: level.AvailableQty == 0,
			}
			return nil
		}

		if _, err := q.UpsertStockLevel(ctx, stockdb.UpsertStockLevelParams{
			TenantID:     tenantUUID,
			SkuID:        req.GetSkuId(),
			WarehouseID:  req.GetWarehouseId(),
			AvailableQty: req.GetQuantityDelta(),
		}); err != nil {
			return err
		}

		level, err := q.GetStockLevelForUpdate(ctx, stockLevelParams(loc))
		if err != nil {
			return err
		}

		clamped := level.AvailableQty < 0
		newAvailable := level.AvailableQty
		if clamped {
			newAvailable = 0
		}

		_, err = q.UpdateStockLevel(ctx, stockdb.UpdateStockLevelParams{
			TenantID:         tenantUUID,
			SkuID:            req.GetSkuId(),
			WarehouseID:      req.GetWarehouseId(),
			AvailableQty:     newAvailable,
			ReservedQty:      level.ReservedQty,
			AllocatedQty:     level.AllocatedQty,
			IsLockedForAudit: level.IsLockedForAudit,
			UpdatedAt:        level.UpdatedAt,
		})
		if err != nil {
			return err
		}

		event, err := q.InsertStockEvent(ctx, stockdb.InsertStockEventParams{
			TenantID:    tenantUUID,
			SkuID:       req.GetSkuId(),
			WarehouseID: req.GetWarehouseId(),
			Delta:       req.GetQuantityDelta(),
			Reason:      req.GetReason(),
			Source:      req.GetSource(),
			ActorID:     "system",
		})
		if err != nil {
			return err
		}

		payload, _ := marshalAdjustEvent(req)
		_, err = q.InsertOutboxEvent(ctx, stockdb.InsertOutboxEventParams{
			TenantID:      tenantUUID,
			AggregateType: "stock",
			AggregateID:   tenantUUID.String() + ":" + req.GetSkuId(),
			EventType:     "stock.adjusted",
			Payload:       payload,
		})
		if err != nil {
			return err
		}

		resp = &stockv1.AdjustStockResponse{
			Success:       true,
			AvailableQty:  newAvailable,
			ReservedQty:   level.ReservedQty,
			AllocatedQty:  level.AllocatedQty,
			IsClampedZero: clamped,
			EventId:       event.ID.String(),
		}

		// US11: emit deficit alert if stock was clamped.
		if clamped {
			deficitPayload, _ := json.Marshal(map[string]any{
				"sku_id":       req.GetSkuId(),
				"warehouse_id": req.GetWarehouseId(),
				"delta":        req.GetQuantityDelta(),
				"reason":       req.GetReason(),
			})
			if _, err := q.InsertOutboxEvent(ctx, stockdb.InsertOutboxEventParams{
				TenantID:      tenantUUID,
				AggregateType: "stock",
				AggregateID:   tenantUUID.String() + ":" + req.GetSkuId(),
				EventType:     "stock.deficit_alert",
				Payload:       deficitPayload,
			}); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "adjust stock: %v", err)
	}
	return resp, nil
}

// ReserveStock atomically reserves multiple lines (all-or-nothing).
func (s *Server) ReserveStock(ctx context.Context, req *stockv1.ReserveStockRequest) (*stockv1.ReserveStockResponse, error) {
	tenantUUID, err := database.ParseUUID(req.GetTenantId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ctx = database.WithTenantID(ctx, req.GetTenantId())
	lines := sortedLines(req.GetLines())
	if len(lines) == 0 {
		return nil, status.Error(codes.InvalidArgument, "no lines provided")
	}

	var resp *stockv1.ReserveStockResponse
	err = database.ExecTxWithTenant(ctx, s.pool, func(tx pgx.Tx) error {
		q := stockdb.New(tx)

		dup, err := tryIdempotency(ctx, q, tenantUUID, req.GetIdempotencyKey(), req)
		if err != nil {
			return err
		}
		if dup {
			resp = &stockv1.ReserveStockResponse{Success: true}
			return nil
		}

		var failed []string
		for _, l := range lines {
			loc := StockLocation{TenantID: tenantUUID, SkuID: l.SkuId, WarehouseID: l.WarehouseId}
			level, err := q.GetStockLevelForUpdate(ctx, stockLevelParams(loc))
			if err == pgx.ErrNoRows || (err == nil && (level.IsLockedForAudit || level.AvailableQty < l.Quantity)) {
				failed = append(failed, l.SkuId)
				continue
			}
			if err != nil {
				return err
			}
			if _, err := q.UpdateStockLevel(ctx, stockdb.UpdateStockLevelParams{
				TenantID:         tenantUUID,
				SkuID:            l.SkuId,
				WarehouseID:      l.WarehouseId,
				AvailableQty:     level.AvailableQty - l.Quantity,
				ReservedQty:      level.ReservedQty + l.Quantity,
				AllocatedQty:     level.AllocatedQty,
				IsLockedForAudit: level.IsLockedForAudit,
				UpdatedAt:        level.UpdatedAt,
			}); err != nil {
				return err
			}
			if _, err := q.InsertStockEvent(ctx, stockdb.InsertStockEventParams{
				TenantID:    tenantUUID,
				SkuID:       l.SkuId,
				WarehouseID: l.WarehouseId,
				Delta:       -l.Quantity,
				Reason:      "reserve",
				Source:      "reservation",
				ActorID:     req.GetOrderId(),
			}); err != nil {
				return err
			}
			if _, err := q.InsertOutboxEvent(ctx, stockdb.InsertOutboxEventParams{
				TenantID:      tenantUUID,
				AggregateType: "stock",
				AggregateID:   tenantUUID.String() + ":" + l.SkuId,
				EventType:     "stock.reserved",
				Payload:       marshalLineEvent("reserve", req.GetOrderId(), l),
			}); err != nil {
				return err
			}
		}

		if len(failed) > 0 {
			resp = &stockv1.ReserveStockResponse{Success: false, FailedSkuIds: failed}
			return errors.New("insufficient stock for reservation")
		}
		resp = &stockv1.ReserveStockResponse{Success: true}
		return nil
	})
	if err != nil {
		if resp != nil && !resp.Success {
			return resp, nil
		}
		return nil, status.Errorf(codes.Internal, "reserve stock: %v", err)
	}
	return resp, nil
}

// ReleaseStock returns reserved units back to available.
func (s *Server) ReleaseStock(ctx context.Context, req *stockv1.ReleaseStockRequest) (*stockv1.ReleaseStockResponse, error) {
	tenantUUID, err := database.ParseUUID(req.GetTenantId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ctx = database.WithTenantID(ctx, req.GetTenantId())
	lines := sortedLines(req.GetLines())

	var resp *stockv1.ReleaseStockResponse
	err = database.ExecTxWithTenant(ctx, s.pool, func(tx pgx.Tx) error {
		q := stockdb.New(tx)
		dup, err := tryIdempotency(ctx, q, tenantUUID, req.GetIdempotencyKey(), req)
		if err != nil {
			return err
		}
		if dup {
			resp = &stockv1.ReleaseStockResponse{Success: true}
			return nil
		}
		var released []string
		for _, l := range lines {
			loc := StockLocation{TenantID: tenantUUID, SkuID: l.SkuId, WarehouseID: l.WarehouseId}
			level, err := q.GetStockLevelForUpdate(ctx, stockLevelParams(loc))
			if err == pgx.ErrNoRows {
				continue
			}
			if err != nil {
				return err
			}
			if _, err := q.UpdateStockLevel(ctx, stockdb.UpdateStockLevelParams{
				TenantID:         tenantUUID,
				SkuID:            l.SkuId,
				WarehouseID:      l.WarehouseId,
				AvailableQty:     level.AvailableQty + l.Quantity,
				ReservedQty:      level.ReservedQty - l.Quantity,
				AllocatedQty:     level.AllocatedQty,
				IsLockedForAudit: level.IsLockedForAudit,
				UpdatedAt:        level.UpdatedAt,
			}); err != nil {
				return err
			}
			if _, err := q.InsertStockEvent(ctx, stockdb.InsertStockEventParams{
				TenantID:    tenantUUID,
				SkuID:       l.SkuId,
				WarehouseID: l.WarehouseId,
				Delta:       l.Quantity,
				Reason:      req.GetReason(),
				Source:      "reservation",
				ActorID:     req.GetOrderId(),
			}); err != nil {
				return err
			}
			if _, err := q.InsertOutboxEvent(ctx, stockdb.InsertOutboxEventParams{
				TenantID:      tenantUUID,
				AggregateType: "stock",
				AggregateID:   tenantUUID.String() + ":" + l.SkuId,
				EventType:     "stock.released",
				Payload:       marshalLineEvent("release", req.GetOrderId(), l),
			}); err != nil {
				return err
			}
			released = append(released, l.SkuId)
		}
		resp = &stockv1.ReleaseStockResponse{Success: true, ReleasedSkuIds: released}
		return nil
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "release stock: %v", err)
	}
	return resp, nil
}

// ConfirmStockAllocation transitions reserved -> allocated for the given lines.
func (s *Server) ConfirmStockAllocation(ctx context.Context, req *stockv1.ConfirmStockAllocationRequest) (*stockv1.ConfirmStockAllocationResponse, error) {
	tenantUUID, err := database.ParseUUID(req.GetTenantId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ctx = database.WithTenantID(ctx, req.GetTenantId())
	lines := sortedLines(req.GetLines())

	var resp *stockv1.ConfirmStockAllocationResponse
	err = database.ExecTxWithTenant(ctx, s.pool, func(tx pgx.Tx) error {
		q := stockdb.New(tx)
		dup, err := tryIdempotency(ctx, q, tenantUUID, req.GetIdempotencyKey(), req)
		if err != nil {
			return err
		}
		if dup {
			resp = &stockv1.ConfirmStockAllocationResponse{Success: true}
			return nil
		}
		var out []*stockv1.StockAllocationLine
		for _, l := range lines {
			loc := StockLocation{TenantID: tenantUUID, SkuID: l.SkuId, WarehouseID: l.WarehouseId}
			level, err := q.GetStockLevelForUpdate(ctx, stockLevelParams(loc))
			if err != nil {
				return err
			}
			if _, err := q.UpdateStockLevel(ctx, stockdb.UpdateStockLevelParams{
				TenantID:         tenantUUID,
				SkuID:            l.SkuId,
				WarehouseID:      l.WarehouseId,
				AvailableQty:     level.AvailableQty,
				ReservedQty:      level.ReservedQty - l.Quantity,
				AllocatedQty:     level.AllocatedQty + l.Quantity,
				IsLockedForAudit: level.IsLockedForAudit,
				UpdatedAt:        level.UpdatedAt,
			}); err != nil {
				return err
			}
			if _, err := q.InsertOutboxEvent(ctx, stockdb.InsertOutboxEventParams{
				TenantID:      tenantUUID,
				AggregateType: "stock",
				AggregateID:   tenantUUID.String() + ":" + l.SkuId,
				EventType:     "stock.allocated",
				Payload:       marshalLineEvent("allocate", req.GetOrderId(), l),
			}); err != nil {
				return err
			}
			out = append(out, &stockv1.StockAllocationLine{
				SkuId:        l.SkuId,
				WarehouseId:  l.WarehouseId,
				AllocatedQty: l.Quantity,
			})
		}
		resp = &stockv1.ConfirmStockAllocationResponse{Success: true, Lines: out}
		return nil
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "confirm allocation: %v", err)
	}
	return resp, nil
}

// UnlockStockForAudit clears an audit lock after recording a reconciliation correction.
func (s *Server) UnlockStockForAudit(ctx context.Context, req *stockv1.UnlockStockForAuditRequest) (*stockv1.UnlockStockForAuditResponse, error) {
	tenantUUID, err := database.ParseUUID(req.GetTenantId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ctx = database.WithTenantID(ctx, req.GetTenantId())
	loc := StockLocation{TenantID: tenantUUID, SkuID: req.GetSkuId(), WarehouseID: req.GetWarehouseId()}

	var resp *stockv1.UnlockStockForAuditResponse
	err = database.ExecTxWithTenant(ctx, s.pool, func(tx pgx.Tx) error {
		q := stockdb.New(tx)
		_, err := q.GetStockLevelForUpdate(ctx, stockLevelParams(loc))
		if err == pgx.ErrNoRows {
			resp = &stockv1.UnlockStockForAuditResponse{Success: false, IsUnlocked: false}
			return nil
		}
		if err != nil {
			return err
		}
		if err := q.SetAuditLock(ctx, stockdb.SetAuditLockParams{
			TenantID:         tenantUUID,
			SkuID:            req.GetSkuId(),
			WarehouseID:      req.GetWarehouseId(),
			IsLockedForAudit: false,
		}); err != nil {
			return err
		}
		actor := req.GetActorId()
		if actor == "" {
			actor = "warehouse-manager"
		}
		if _, err := q.InsertStockEvent(ctx, stockdb.InsertStockEventParams{
			TenantID:    tenantUUID,
			SkuID:       req.GetSkuId(),
			WarehouseID: req.GetWarehouseId(),
			Delta:       0,
			Reason:      req.GetReason(),
			Source:      "reconciliation",
			ActorID:     actor,
		}); err != nil {
			return err
		}
		resp = &stockv1.UnlockStockForAuditResponse{Success: true, IsUnlocked: true}
		return nil
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "unlock for audit: %v", err)
	}
	return resp, nil
}
