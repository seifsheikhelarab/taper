package reservationservice

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	resdb "github.com/seifsheikhelarab/taper/gen/go/db/reservation"
)

var (
	tenantT1 = "11111111-1111-1111-1111-111111111111"
	tenantT2 = "22222222-2222-2222-2222-222222222222"
)

func resRow(tenant, order, sku, wh string, qty int32) resdb.Reservation {
	var tid pgtype.UUID
	if err := tid.Scan(tenant); err != nil {
		panic(err) // test fixture: tenants are fixed UUID strings
	}
	return resdb.Reservation{TenantID: tid, OrderID: order, SkuID: sku, WarehouseID: wh, Quantity: qty, ExpiresAt: pgtypeTimestamptz(time.Now())}
}

// Grouping drives which stock lines ride one release call: rows for the
// same (tenant, order) land in one group; other tenants/orders stay apart.
func TestGroupByOrder(t *testing.T) {
	rows := []resdb.Reservation{
		resRow(tenantT1, "o1", "SKU-1", "W1", 2),
		resRow(tenantT1, "o1", "SKU-2", "W1", 3),
		resRow(tenantT1, "o2", "SKU-1", "W1", 1),
		resRow(tenantT2, "o1", "SKU-1", "W1", 9),
	}
	groups := groupByOrder(rows)
	if len(groups) != 3 {
		t.Fatalf("groups = %d, want 3", len(groups))
	}
	o1 := groups[groupKey{tenant: tenantT1, orderID: "o1"}]
	if len(o1) != 2 {
		t.Fatalf("t1/o1 lines = %d, want 2", len(o1))
	}
	if o1[0].SkuID != "SKU-1" || o1[1].SkuID != "SKU-2" {
		t.Fatalf("t1/o1 lines = %v/%v, want SKU-1/SKU-2", o1[0].SkuID, o1[1].SkuID)
	}
}

// The release request carries every grouped line and the compensation key,
// so a sweeper replay is exactly-once at the stock service.
func TestReleaseRequest(t *testing.T) {
	rows := []resdb.Reservation{
		resRow(tenantT1, "o1", "SKU-1", "W1", 2),
		resRow(tenantT1, "o1", "SKU-2", "W2", 3),
	}
	req := releaseRequest(tenantT1, "o1", rows)
	if req.GetTenantId() != tenantT1 || req.GetOrderId() != "o1" {
		t.Fatalf("target = %s/%s", req.GetTenantId(), req.GetOrderId())
	}
	if req.GetReason() != "ttl_expiry" {
		t.Fatalf("reason = %q, want ttl_expiry", req.GetReason())
	}
	if len(req.GetLines()) != 2 {
		t.Fatalf("lines = %d, want 2", len(req.GetLines()))
	}
	if req.GetLines()[0].Quantity != 2 || req.GetLines()[1].WarehouseId != "W2" {
		t.Fatalf("lines = %+v", req.GetLines())
	}
	// The compensation key differs from the caller-supplied idempotency key
	// (database.CompensationKey hashes tenant+order), so the sweeper's
	// release is never suppressed by a prior Reserve idempotency record.
	if req.GetIdempotencyKey() == "" {
		t.Fatal("compensation key is required")
	}
}

// Empty groups produce a request with no lines (the stock service treats
// it as a no-op) rather than a panic.
func TestReleaseRequestEmpty(t *testing.T) {
	req := releaseRequest("t", "o", nil)
	if len(req.GetLines()) != 0 {
		t.Fatalf("lines = %d, want 0", len(req.GetLines()))
	}
}
