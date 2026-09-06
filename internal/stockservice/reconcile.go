// Reconciliation worker: ADR-0001 nightly drift detection. Replays
// stock_events per (tenant, sku, warehouse), derives every stock_levels
// bucket, and locks drifted rows for audit until a warehouse manager clears
// them via UnlockStockForAudit.
//
// Event derivation contract (reason -> bucket effect):
//
//	reason "adjust", "external" ...  -> available += delta
//	reason "reserve"                 -> available += delta (=-qty), reserved += -delta
//	reason "release"                 -> available += delta (=+qty), reserved += -delta
//	reason "allocate" (marker)       -> reserved += -delta, allocated += delta
//	reason "fulfill" (marker)        -> allocated += -delta
//
// Reserve/release move goods between sellable stock and the reservation
// bucket, so they affect available AND reserved. Markers (allocate/fulfill)
// never touch available. Zero-delta rows (e.g. RECONCILIATION_CORRECTION
// unlocks) affect nothing.
package stockservice

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	stockdb "github.com/seifsheikhelarab/taper/gen/go/db/stock"
	"github.com/seifsheikhelarab/taper/pkg/config"
)

// Drift records one location whose derived buckets disagree with stock_levels.
type Drift struct {
	TenantID          string
	SKUID             string
	WarehouseID       string
	ExpectedAvailable int32
	ActualAvailable   int32
	ExpectedReserved  int32
	ActualReserved    int32
	ExpectedAllocated int32
	ActualAllocated   int32
}

// Worker runs the nightly reconciliation pass.
type Worker struct {
	pool *pgxpool.Pool
	log  func(format string, args ...any)
}

// New creates a worker over the stock database. The pool must use a
// BYPASSRLS role: reconciliation is a cross-tenant maintenance task.
func New(pool *pgxpool.Pool, log func(format string, args ...any)) *Worker {
	if log == nil {
		log = func(string, ...any) {}
	}
	return &Worker{pool: pool, log: log}
}

// Run blocks, reconciling once at startup and then on the interval from
// RECONCILE_INTERVAL (default 24h), until ctx is cancelled. Does nothing
// unless RECONCILE_ENABLED is truthy.
func (w *Worker) Run(ctx context.Context) {
	if !config.EnvBool("RECONCILE_ENABLED") {
		w.log("reconcile: disabled (RECONCILE_ENABLED not set)")
		return
	}
	interval := config.EnvDuration("RECONCILE_INTERVAL", 24*time.Hour)
	w.pass(ctx)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.pass(ctx)
		}
	}
}

// pass executes one reconciliation cycle: derive, compare, lock drifts.
func (w *Worker) pass(ctx context.Context) {
	drifts, err := w.Reconcile(ctx)
	if err != nil {
		w.log("reconcile: pass failed: %v", err)
		return
	}
	if len(drifts) > 0 {
		w.log("reconcile: %d location(s) drifted; audit locks applied", len(drifts))
	} else {
		w.log("reconcile: no drift")
	}
}

// Reconcile derives buckets from stock_events for every location with
// events, compares against stock_levels, and sets is_locked_for_audit on
// drifted rows. Returns the drifts found.
func (w *Worker) Reconcile(ctx context.Context) ([]Drift, error) {
	q := stockdb.New(w.pool)
	locs, err := q.ListDistinctEventLocations(ctx)
	if err != nil {
		return nil, err
	}
	var drifts []Drift
	for _, loc := range locs {
		levels, err := q.GetStockLevelAny(ctx, stockdb.GetStockLevelAnyParams{
			TenantID:    loc.TenantID,
			SkuID:       loc.SkuID,
			WarehouseID: loc.WarehouseID,
		})
		if err != nil {
			continue // location without a stock_levels row: nothing to compare
		}
		reasons, err := q.SumDeltasByReason(ctx, stockdb.SumDeltasByReasonParams{
			TenantID:    loc.TenantID,
			SkuID:       loc.SkuID,
			WarehouseID: loc.WarehouseID,
		})
		if err != nil {
			return nil, err
		}
		var avail, reserved, allocated int64
		for _, r := range reasons {
			switch r.Reason {
			case "allocate":
				reserved -= r.TotalDelta
				allocated += r.TotalDelta
			case "fulfill":
				allocated -= r.TotalDelta
			case "reserve", "release":
				reserved -= r.TotalDelta
				avail += r.TotalDelta
			default:
				avail += r.TotalDelta
			}
		}
		d := Drift{
			TenantID:          loc.TenantID.String(),
			SKUID:             loc.SkuID,
			WarehouseID:       loc.WarehouseID,
			ExpectedAvailable: int32(avail),
			ActualAvailable:   levels.AvailableQty,
			ExpectedReserved:  int32(reserved),
			ActualReserved:    levels.ReservedQty,
			ExpectedAllocated: int32(allocated),
			ActualAllocated:   levels.AllocatedQty,
		}
		if d.ExpectedAvailable != d.ActualAvailable ||
			d.ExpectedReserved != d.ActualReserved ||
			d.ExpectedAllocated != d.ActualAllocated {
			if err := q.SetAuditLock(ctx, stockdb.SetAuditLockParams{
				TenantID:         loc.TenantID,
				SkuID:            loc.SkuID,
				WarehouseID:      loc.WarehouseID,
				IsLockedForAudit: true,
			}); err != nil {
				return nil, err
			}
			drifts = append(drifts, d)
		}
	}
	return drifts, nil
}
