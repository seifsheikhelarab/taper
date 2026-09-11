package orderservice

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	orderdb "github.com/seifsheikhelarab/taper/gen/go/db/order"
	orderv1 "github.com/seifsheikhelarab/taper/gen/go/order/v1"
	resv1 "github.com/seifsheikhelarab/taper/gen/go/reservation/v1"
	stockv1 "github.com/seifsheikhelarab/taper/gen/go/stock/v1"
	"github.com/seifsheikhelarab/taper/pkg/circuitbreaker"
	"github.com/seifsheikhelarab/taper/pkg/database"
	"github.com/seifsheikhelarab/taper/pkg/payment"
)

// Saga drives the orchestrated order flow:
//
//	CreateOrder -> Reserve (gRPC) -> Charge (gateway) -> Allocate (gRPC) -> CONFIRMED
//	                                     \-> on failure: compensate (release) -> COMPENSATED
//
// State is persisted in saga_instances at every step so a crash resumes
// exactly-once without double-charging or double-allocating (spec US5).
type Saga struct {
	orderv1.UnimplementedOrderServiceServer
	pool         *pgxpool.Pool
	sweeperPool  *pgxpool.Pool // BYPASSRLS role for cross-tenant resume scans
	reservations resv1.ReservationServiceClient
	stock        stockv1.StockServiceClient
	gateway      *payment.Sandbox
	defaultTTL   time.Duration
	// Fail-Fast Policy: fail immediately when downstream deps are down.
	resBreaker   *circuitbreaker.Breaker
	payBreaker   *circuitbreaker.Breaker
	stockBreaker *circuitbreaker.Breaker
}

func NewSaga(pool *pgxpool.Pool, sweeperPool *pgxpool.Pool, res resv1.ReservationServiceClient, stock stockv1.StockServiceClient, gw *payment.Sandbox) *Saga {
	return &Saga{
		pool:         pool,
		sweeperPool:  sweeperPool,
		reservations: res,
		stock:        stock,
		gateway:      gw,
		defaultTTL:   15 * time.Minute,
		resBreaker:   circuitbreaker.New(circuitbreaker.Config{FailureThreshold: 3, Cooldown: 10 * time.Second}),
		payBreaker:   circuitbreaker.New(circuitbreaker.Config{FailureThreshold: 3, Cooldown: 10 * time.Second}),
		stockBreaker: circuitbreaker.New(circuitbreaker.Config{FailureThreshold: 3, Cooldown: 10 * time.Second}),
	}
}

var errIdempotentReplay = errors.New("idempotency key already processed")

// sagaInput carries everything runSaga needs; it is built either from a
// CreateOrder request or from persisted rows during crash resume.
type sagaInput struct {
	TenantID            string
	TenantUUID          pgtype.UUID
	OrderID             string
	Lines               []*orderv1.OrderLine
	IdemKey             string
	TTL                 time.Duration
	ForcePaymentFailure bool
}

// CreateOrder validates the request, persists the saga anchor, then runs the
// synchronous reserve -> pay -> allocate saga.
func (s *Saga) CreateOrder(ctx context.Context, req *orderv1.CreateOrderRequest) (*orderv1.CreateOrderResponse, error) {
	tenantUUID, err := database.ParseUUID(req.GetTenantId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ctx = database.WithTenantID(ctx, req.GetTenantId())
	if req.GetOrderId() == "" {
		return nil, status.Error(codes.InvalidArgument, "order_id is required")
	}
	if len(req.GetLines()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "no lines provided")
	}
	if orderTotal(req.GetLines()) <= 0 {
		return nil, status.Error(codes.InvalidArgument, "order total must be positive")
	}

	ttl := s.defaultTTL
	if req.GetTtlSeconds() > 0 {
		ttl = time.Duration(req.GetTtlSeconds()) * time.Second
	}

	// Persist order + saga + idempotency anchor in one local transaction.
	_, err = s.createOrderRow(ctx, tenantUUID, req, orderTotal(req.GetLines()))
	if err != nil {
		if errors.Is(err, errIdempotentReplay) {
			// Return the outcome of the original run.
			return s.replayCreateOrder(ctx, req)
		}
		return nil, status.Errorf(codes.Internal, "create order: %v", err)
	}

	return s.runSaga(ctx, sagaInput{
		TenantID:            req.GetTenantId(),
		TenantUUID:          tenantUUID,
		OrderID:             req.GetOrderId(),
		Lines:               req.GetLines(),
		IdemKey:             req.GetIdempotencyKey(),
		TTL:                 ttl,
		ForcePaymentFailure: req.GetForcePaymentFailure(),
	})
}

// ResumePendingSagas re-drives sagas stuck in a non-terminal state after a
// crash (spec US5: durable state so a crashed orchestrator resumes
// exactly-once). Safe to call periodically; every step is idempotently keyed.
func (s *Saga) ResumePendingSagas(ctx context.Context, limit int32) error {
	type resumeRow struct {
		TenantID       pgtype.UUID
		OrderID        string
		IdempotencyKey string
	}
	var rows []resumeRow
	err := database.ExecTxNoTenant(ctx, s.sweeperPool, func(tx pgx.Tx) error {
		// Cross-tenant maintenance query (FOR UPDATE SKIP LOCKED) via the
		// BYPASSRLS sweeper role, mirroring the Phase 1 TTL sweeper. Normal
		// request traffic stays RLS-scoped on the app pool.
		got, err := orderdb.New(tx).GetResumableSagas(ctx, limit)
		if err != nil {
			return err
		}
		rows = make([]resumeRow, 0, len(got))
		for _, g := range got {
			rows = append(rows, resumeRow{TenantID: g.TenantID, OrderID: g.OrderID, IdempotencyKey: g.IdempotencyKey})
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("scan resumable sagas: %w", err)
	}
	for _, r := range rows {
		sagaCtx := database.WithTenantID(ctx, r.TenantID.String())
		// Reload lines from the order row; amounts and TTL were persisted.
		var lines []*orderv1.OrderLine
		err := database.ExecTxWithTenant(sagaCtx, s.pool, func(tx pgx.Tx) error {
			got, err := orderdb.New(tx).GetOrderLines(ctx, r.OrderID)
			if err != nil {
				return err
			}
			lines = make([]*orderv1.OrderLine, 0, len(got))
			for _, l := range got {
				lines = append(lines, &orderv1.OrderLine{
					SkuId:       l.SkuID,
					WarehouseId: l.WarehouseID,
					Quantity:    l.Quantity,
					UnitPrice:   l.UnitPrice,
				})
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("resume %s: load lines: %w", r.OrderID, err)
		}
		if _, err := s.runSaga(sagaCtx, sagaInput{
			TenantID:   r.TenantID.String(),
			TenantUUID: r.TenantID,
			OrderID:    r.OrderID,
			Lines:      lines,
			IdemKey:    r.IdempotencyKey,
			TTL:        s.defaultTTL,
		}); err != nil {
			// Keep resuming the rest; the failed saga stays non-terminal and
			// will be picked up on the next pass.
			continue
		}
	}
	return nil
}

// runSaga executes (or replays) the reserve -> pay -> allocate -> confirm
// steps. Every downstream step is keyed by the order idempotency key plus a
// step suffix, so replaying after a crash is exactly-once at each dependency.
func (s *Saga) runSaga(ctx context.Context, in sagaInput) (*orderv1.CreateOrderResponse, error) {
	// Step 1: Reserve (all-or-nothing via reservation service).
	// Fail-Fast Policy: fail immediately when the breaker is open.
	if err := s.resBreaker.Allow(); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	resResp, err := s.reservations.Reserve(ctx, &resv1.ReserveRequest{
		TenantId:       in.TenantID,
		OrderId:        in.OrderID,
		TtlSeconds:     int32(in.TTL.Seconds()),
		IdempotencyKey: in.IdemKey + ":reserve",
		Items:          reserveItems(in.Lines),
	})
	if err != nil {
		s.resBreaker.RecordFailure()
		_ = s.failSaga(ctx, in.TenantID, in.OrderID, "reserve: "+err.Error())
		return nil, status.Errorf(codes.Internal, "reserve: %v", err)
	}
	s.resBreaker.RecordSuccess()
	if !resResp.GetSuccess() {
		_ = s.failSaga(ctx, in.TenantID, in.OrderID, "insufficient stock")
		return &orderv1.CreateOrderResponse{
			OrderId: in.OrderID,
			Status:  SagaFailed,
		}, nil
	}
	// On crash-resume of an already-RESERVED saga this re-emits order.reserved;
	// outbox consumers are at-least-once, so a duplicate is safe.
	if err := s.transition(ctx, in.TenantUUID, in.OrderID, SagaReserved, 1, ""); err != nil {
		return nil, status.Errorf(codes.Internal, "saga transition: %v", err)
	}

	// Step 2: Charge payment.
	var chargeErr error
	var txnID string
	if in.ForcePaymentFailure {
		chargeErr = payment.ErrPaymentDeclined
	} else {
		if err := s.payBreaker.Allow(); err != nil {
			return nil, status.Error(codes.Unavailable, err.Error())
		}
		res, cerr := s.gateway.Charge(ctx, payment.ChargeRequest{
			OrderID:  in.OrderID,
			TenantID: in.TenantID,
			Amount:   orderTotal(in.Lines),
			Currency: "USD",
			IdemKey:  in.IdemKey + ":charge",
		})
		if cerr == nil && res != nil {
			txnID = res.TransactionID
		}
		chargeErr = cerr
		// A declined payment is a business outcome, not a dependency failure:
		// only infrastructure errors trip the breaker.
		if chargeErr != nil && !errors.Is(chargeErr, payment.ErrPaymentDeclined) {
			s.payBreaker.RecordFailure()
		} else {
			s.payBreaker.RecordSuccess()
		}
	}
	if chargeErr != nil {
		// Compensate: release the held stock, mark compensated.
		compErr := s.compensate(ctx, in.TenantID, in.OrderID, "payment_failed")
		if compErr != nil {
			_ = s.failSaga(ctx, in.TenantID, in.OrderID, "payment failed AND compensation error: "+compErr.Error())
			return nil, status.Errorf(codes.Internal, "compensate: %v", compErr)
		}
		return &orderv1.CreateOrderResponse{
			OrderId:     in.OrderID,
			Status:      SagaCompensated,
			TotalAmount: orderTotal(in.Lines),
		}, nil
	}

	// Persist the transaction reference before allocating.
	if err := s.recordTransaction(ctx, in.TenantUUID, in.OrderID, txnID); err != nil {
		return nil, status.Errorf(codes.Internal, "record transaction: %v", err)
	}

	// Step 3: Allocate (Reserved -> Allocated; sweeper can no longer release).
	if _, err := s.reservations.AllocateReservation(ctx, &resv1.AllocateReservationRequest{
		TenantId:       in.TenantID,
		OrderId:        in.OrderID,
		IdempotencyKey: in.IdemKey + ":allocate",
	}); err != nil {
		s.resBreaker.RecordFailure()
		_ = s.failSaga(ctx, in.TenantID, in.OrderID, "allocate: "+err.Error())
		return nil, status.Errorf(codes.Internal, "allocate: %v", err)
	}
	s.resBreaker.RecordSuccess()
	// Transition the stock-level rows reserved -> allocated (US5: post-payment
	// inventory must not sit in reserved where the sweeper could release it).
	if err := s.stockBreaker.Allow(); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	if _, err := s.stock.ConfirmStockAllocation(ctx, &stockv1.ConfirmStockAllocationRequest{
		TenantId:       in.TenantID,
		OrderId:        in.OrderID,
		IdempotencyKey: in.IdemKey + ":confirm",
		Lines:          stockLines(in.Lines),
	}); err != nil {
		s.stockBreaker.RecordFailure()
		_ = s.failSaga(ctx, in.TenantID, in.OrderID, "confirm: "+err.Error())
		return nil, status.Errorf(codes.Internal, "confirm: %v", err)
	}
	s.stockBreaker.RecordSuccess()
	if err := s.transition(ctx, in.TenantUUID, in.OrderID, SagaConfirmed, 3, ""); err != nil {
		return nil, status.Errorf(codes.Internal, "saga transition: %v", err)
	}

	return &orderv1.CreateOrderResponse{
		OrderId:       in.OrderID,
		Status:        SagaConfirmed,
		TotalAmount:   orderTotal(in.Lines),
		TransactionId: txnID,
	}, nil
}

// GetOrder returns order and saga state.
func (s *Saga) GetOrder(ctx context.Context, req *orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
	if _, err := database.ParseUUID(req.GetTenantId()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ctx = database.WithTenantID(ctx, req.GetTenantId())

	var out *orderv1.GetOrderResponse
	err := database.ExecTxWithTenant(ctx, s.pool, func(tx pgx.Tx) error {
		q := orderdb.New(tx)
		o, err := q.GetOrderById(ctx, req.GetOrderId())
		if errors.Is(err, pgx.ErrNoRows) {
			return status.Error(codes.NotFound, "order not found")
		}
		if err != nil {
			return err
		}
		lines, err := q.GetOrderLines(ctx, req.GetOrderId())
		if err != nil {
			return err
		}
		saga, err := q.GetSagaInstance(ctx, req.GetOrderId())
		if err != nil {
			return err
		}
		pbLines := make([]*orderv1.OrderLine, 0, len(lines))
		for _, l := range lines {
			pbLines = append(pbLines, &orderv1.OrderLine{
				SkuId:       l.SkuID,
				WarehouseId: l.WarehouseID,
				Quantity:    l.Quantity,
				UnitPrice:   l.UnitPrice,
			})
		}
		var createdAt int64
		if o.CreatedAt.Valid {
			createdAt = o.CreatedAt.Time.Unix()
		}
		out = &orderv1.GetOrderResponse{
			OrderId:       o.OrderID,
			Status:        o.Status,
			TotalAmount:   o.TotalAmount,
			Lines:         pbLines,
			SagaState:     saga.State,
			TransactionId: o.TransactionID,
			CreatedAtUnix: createdAt,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// CancelOrder compensates an order still inside its payment window.
func (s *Saga) CancelOrder(ctx context.Context, req *orderv1.CancelOrderRequest) (*orderv1.CancelOrderResponse, error) {
	tenantUUID, err := database.ParseUUID(req.GetTenantId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ctx = database.WithTenantID(ctx, req.GetTenantId())

	// Only cancellable before allocation/confirmation.
	saga, err := s.getSaga(ctx, tenantUUID, req.GetOrderId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get saga: %v", err)
	}
	if saga.State == SagaAllocated || saga.State == SagaConfirmed {
		return &orderv1.CancelOrderResponse{Success: false, Status: saga.State}, nil
	}
	if saga.State == SagaCompensated || saga.State == SagaFailed {
		return &orderv1.CancelOrderResponse{Success: true, Status: saga.State}, nil
	}

	if err := s.compensate(ctx, req.GetTenantId(), req.GetOrderId(), reasonOrDefault(req.GetReason(), "customer_cancel")); err != nil {
		return nil, status.Errorf(codes.Internal, "compensate: %v", err)
	}
	return &orderv1.CancelOrderResponse{Success: true, Status: SagaCompensated}, nil
}

// FulfillOrder transitions a CONFIRMED order to FULFILLED and emits the
// order.fulfilled outbox event that drives stock fulfillment. Replays with
// the same idempotency payload are successful no-ops; anything not
// CONFIRMED (or already-FULFILLED with a different payload) is rejected.
func (s *Saga) FulfillOrder(ctx context.Context, req *orderv1.FulfillOrderRequest) (*orderv1.FulfillOrderResponse, error) {
	tenantUUID, err := database.ParseUUID(req.GetTenantId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ctx = database.WithTenantID(ctx, req.GetTenantId())

	var resp *orderv1.FulfillOrderResponse
	err = database.ExecTxWithTenant(ctx, s.pool, func(tx pgx.Tx) error {
		q := orderdb.New(tx)
		o, err := q.GetOrderById(ctx, req.GetOrderId())
		if errors.Is(err, pgx.ErrNoRows) {
			return status.Error(codes.NotFound, "order not found")
		}
		if err != nil {
			return err
		}
		// Already FULFILLED: only the original replay (same idempotency key,
		// same payload hash) succeeds as a no-op; anything else is rejected
		// so a second, distinct fulfillment request cannot masquerade as a
		// replay.
		if o.Status == OrderFulfilled {
			if req.GetIdempotencyKey() == "" {
				return status.Errorf(codes.FailedPrecondition, "order %s is already FULFILLED", req.GetOrderId())
			}
			storedHash, err := q.GetIdempotencyKey(ctx, orderdb.GetIdempotencyKeyParams{
				TenantID:       tenantUUID,
				IdempotencyKey: req.GetIdempotencyKey(),
			})
			if errors.Is(err, pgx.ErrNoRows) {
				return status.Errorf(codes.FailedPrecondition, "order %s is already FULFILLED", req.GetOrderId())
			}
			if err != nil {
				return err
			}
			if storedHash != database.PayloadHash(req) {
				return status.Errorf(codes.FailedPrecondition, "order %s is already FULFILLED (payload mismatch)", req.GetOrderId())
			}
			resp = &orderv1.FulfillOrderResponse{Success: true, Status: OrderFulfilled}
			return nil
		}
		if o.Status != OrderConfirmed {
			return status.Errorf(codes.FailedPrecondition, "order %s is %s, must be CONFIRMED", req.GetOrderId(), o.Status)
		}
		if req.GetIdempotencyKey() != "" {
			if _, err := q.CheckAndInsertIdempotencyKey(ctx, orderdb.CheckAndInsertIdempotencyKeyParams{
				TenantID:       tenantUUID,
				IdempotencyKey: req.GetIdempotencyKey(),
				PayloadHash:    database.PayloadHash(req),
			}); errors.Is(err, pgx.ErrNoRows) {
				// Key already recorded: only a genuine fulfillment replay
				// (same payload hash) is a no-op success; a cross-RPC key
				// collision must not fake a fulfillment.
				storedHash, gerr := q.GetIdempotencyKey(ctx, orderdb.GetIdempotencyKeyParams{
					TenantID:       tenantUUID,
					IdempotencyKey: req.GetIdempotencyKey(),
				})
				if gerr != nil {
					return gerr
				}
				if storedHash == database.PayloadHash(req) {
					resp = &orderv1.FulfillOrderResponse{Success: true, Status: OrderFulfilled}
					return nil
				}
				return status.Errorf(codes.FailedPrecondition, "idempotency key %s already used with a different payload", req.GetIdempotencyKey())
			} else if err != nil {
				return err
			}
		}
		lines, err := q.GetOrderLines(ctx, req.GetOrderId())
		if err != nil {
			return err
		}
		evLines := make([]*orderv1.OrderLine, 0, len(lines))
		for _, l := range lines {
			evLines = append(evLines, &orderv1.OrderLine{
				SkuId:       l.SkuID,
				WarehouseId: l.WarehouseID,
				Quantity:    l.Quantity,
			})
		}
		if _, err := q.UpdateOrderStatus(ctx, orderdb.UpdateOrderStatusParams{
			OrderID:       req.GetOrderId(),
			Status:        OrderFulfilled,
			TransactionID: o.TransactionID,
		}); err != nil {
			return err
		}
		if _, err := q.InsertOutboxEvent(ctx, orderdb.InsertOutboxEventParams{
			TenantID:      tenantUUID,
			AggregateType: "order",
			AggregateID:   tenantUUID.String() + ":" + req.GetOrderId(),
			EventType:     "order.fulfilled",
			Payload:       marshalOrderEvent(OrderFulfilled, req.GetOrderId(), o.TotalAmount, evLines),
			Traceparent:   database.TraceparentText(ctx),
		}); err != nil {
			return err
		}
		resp = &orderv1.FulfillOrderResponse{Success: true, Status: OrderFulfilled}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// compensate releases held stock and marks the saga compensated.
func (s *Saga) compensate(ctx context.Context, tenantID, orderID, reason string) error {
	tenantUUID, err := database.ParseUUID(tenantID)
	if err != nil {
		return err
	}
	// Release via reservation service using the compensation key convention
	// so the release is not suppressed as a duplicate by the stock service.
	if _, err := s.reservations.Release(ctx, &resv1.ReleaseRequest{
		TenantId:       tenantID,
		OrderId:        orderID,
		Reason:         reason,
		IdempotencyKey: database.CompensationKey(tenantID, orderID),
	}); err != nil {
		return fmt.Errorf("release: %w", err)
	}
	return s.transition(ctx, tenantUUID, orderID, SagaCompensated, -1, reason)
}

// failSaga marks the saga FAILED with an error message.
func (s *Saga) failSaga(ctx context.Context, tenantID, orderID, msg string) error {
	tenantUUID, err := database.ParseUUID(tenantID)
	if err != nil {
		return err
	}
	return s.transition(ctx, tenantUUID, orderID, SagaFailed, -1, msg)
}

// transition durably moves the saga to a new state and emits an order outbox event.
func (s *Saga) transition(ctx context.Context, tenantUUID pgtype.UUID, orderID, state string, step int32, errMsg string) error {
	return database.ExecTxWithTenant(ctx, s.pool, func(tx pgx.Tx) error {
		q := orderdb.New(tx)
		// Read the current order row so terminal-state mirrors don't clobber the
		// transaction id recorded after the payment step.
		cur, err := q.GetOrderById(ctx, orderID)
		if err != nil {
			return err
		}
		_, err = q.UpdateSagaState(ctx, orderdb.UpdateSagaStateParams{
			OrderID: orderID,
			State:   state,
			Step:    step,
			Error:   errMsg,
		})
		if err != nil {
			return err
		}
		if _, err := q.InsertOutboxEvent(ctx, orderdb.InsertOutboxEventParams{
			TenantID:      tenantUUID,
			AggregateType: "order",
			AggregateID:   tenantUUID.String() + ":" + orderID,
			EventType:     "order." + strings.ToLower(state),
			Payload:       marshalOrderEvent(state, orderID, cur.TotalAmount, nil),
			Traceparent:   database.TraceparentText(ctx),
		}); err != nil {
			return err
		}
		// Mirror terminal states onto the orders row, preserving the transaction id.
		switch state {
		case SagaConfirmed:
			if _, err := q.UpdateOrderStatus(ctx, orderdb.UpdateOrderStatusParams{
				OrderID:       orderID,
				Status:        OrderConfirmed,
				TransactionID: cur.TransactionID,
			}); err != nil {
				return err
			}
		case SagaCompensated:
			if _, err := q.UpdateOrderStatus(ctx, orderdb.UpdateOrderStatusParams{
				OrderID:       orderID,
				Status:        OrderCompensated,
				TransactionID: "",
			}); err != nil {
				return err
			}
		case SagaFailed:
			if _, err := q.UpdateOrderStatus(ctx, orderdb.UpdateOrderStatusParams{
				OrderID:       orderID,
				Status:        OrderFailed,
				TransactionID: "",
			}); err != nil {
				return err
			}
		}
		return nil
	})
}

// createOrderRow persists the order, lines, saga, and idempotency anchor.
// Returns errIdempotentReplay when the (tenant, idempotency_key) was already processed.
func (s *Saga) createOrderRow(ctx context.Context, tenantUUID pgtype.UUID, req *orderv1.CreateOrderRequest, total int64) (pgtype.UUID, error) {
	var id pgtype.UUID
	err := database.ExecTxWithTenant(ctx, s.pool, func(tx pgx.Tx) error {
		q := orderdb.New(tx)
		if req.GetIdempotencyKey() != "" {
			_, err := q.CheckAndInsertIdempotencyKey(ctx, orderdb.CheckAndInsertIdempotencyKeyParams{
				TenantID:       tenantUUID,
				IdempotencyKey: req.GetIdempotencyKey(),
				PayloadHash:    database.PayloadHash(req),
			})
			if errors.Is(err, pgx.ErrNoRows) {
				return errIdempotentReplay
			}
			if err != nil {
				return err
			}
		}
		o, err := q.UpsertOrder(ctx, orderdb.UpsertOrderParams{
			TenantID:    tenantUUID,
			OrderID:     req.GetOrderId(),
			Status:      OrderPending,
			TotalAmount: total,
			Currency:    "USD",
		})
		if err != nil {
			return err
		}
		id = o.ID
		for _, l := range req.GetLines() {
			if _, err := q.InsertOrderLine(ctx, orderdb.InsertOrderLineParams{
				TenantID:    tenantUUID,
				OrderID:     req.GetOrderId(),
				SkuID:       l.GetSkuId(),
				WarehouseID: l.GetWarehouseId(),
				Quantity:    l.GetQuantity(),
				UnitPrice:   l.GetUnitPrice(),
			}); err != nil {
				return err
			}
		}
		if _, err := q.UpsertSagaInstance(ctx, orderdb.UpsertSagaInstanceParams{
			TenantID:       tenantUUID,
			OrderID:        req.GetOrderId(),
			State:          SagaPendingPayment,
			Step:           0,
			IdempotencyKey: req.GetIdempotencyKey(),
		}); err != nil {
			return err
		}
		return nil
	})
	return id, err
}

// replayCreateOrder returns the recorded outcome of a prior CreateOrder attempt.
func (s *Saga) replayCreateOrder(ctx context.Context, req *orderv1.CreateOrderRequest) (*orderv1.CreateOrderResponse, error) {
	var saga orderdb.SagaInstance
	err := database.ExecTxWithTenant(ctx, s.pool, func(tx pgx.Tx) error {
		row, err := orderdb.New(tx).GetSagaInstance(ctx, req.GetOrderId())
		saga = row
		return err
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "replay: %v", err)
	}
	return &orderv1.CreateOrderResponse{
		OrderId: req.GetOrderId(),
		Status:  saga.State,
	}, nil
}

// recordTransaction stores the payment reference on the order row.
func (s *Saga) recordTransaction(ctx context.Context, tenantUUID pgtype.UUID, orderID, txn string) error {
	return database.ExecTxWithTenant(ctx, s.pool, func(tx pgx.Tx) error {
		_, err := orderdb.New(tx).UpdateOrderStatus(ctx, orderdb.UpdateOrderStatusParams{
			OrderID:       orderID,
			Status:        OrderPending,
			TransactionID: txn,
		})
		return err
	})
}

// getSaga fetches the saga instance for an order.
func (s *Saga) getSaga(ctx context.Context, tenantUUID pgtype.UUID, orderID string) (orderdb.SagaInstance, error) {
	var saga orderdb.SagaInstance
	err := database.ExecTxWithTenant(ctx, s.pool, func(tx pgx.Tx) error {
		row, err := orderdb.New(tx).GetSagaInstance(ctx, orderID)
		saga = row
		return err
	})
	return saga, err
}

func reserveItems(lines []*orderv1.OrderLine) []*resv1.ReservationItem {
	items := make([]*resv1.ReservationItem, 0, len(lines))
	for _, l := range lines {
		items = append(items, &resv1.ReservationItem{
			SkuId:       l.GetSkuId(),
			WarehouseId: l.GetWarehouseId(),
			Quantity:    l.GetQuantity(),
		})
	}
	return items
}

func stockLines(lines []*orderv1.OrderLine) []*stockv1.StockLine {
	out := make([]*stockv1.StockLine, 0, len(lines))
	for _, l := range lines {
		out = append(out, &stockv1.StockLine{
			SkuId:       l.GetSkuId(),
			WarehouseId: l.GetWarehouseId(),
			Quantity:    l.GetQuantity(),
		})
	}
	return out
}

func reasonOrDefault(r, def string) string {
	if r == "" {
		return def
	}
	return r
}
