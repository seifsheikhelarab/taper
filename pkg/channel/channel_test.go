package channel

import (
	"context"
	"errors"
	"testing"
)

func TestSandboxSetAvailabilityUpserts(t *testing.T) {
	s := NewSandbox(nil)
	ctx := context.Background()

	if err := s.SetAvailability(ctx, AvailabilityUpdate{TenantID: "t1", SKUID: "SKU-1", WarehouseID: "W1", AvailableQty: 5}); err != nil {
		t.Fatalf("SetAvailability: %v", err)
	}
	if err := s.SetAvailability(ctx, AvailabilityUpdate{TenantID: "t1", SKUID: "SKU-1", WarehouseID: "W1", AvailableQty: 9}); err != nil {
		t.Fatalf("SetAvailability second: %v", err)
	}
	got, ok := s.Availability("t1", "SKU-1", "W1")
	if !ok || got.AvailableQty != 9 {
		t.Fatalf("want last-write-wins qty=9, got %+v ok=%v", got, ok)
	}
	// Different location does not collide.
	if _, ok := s.Availability("t1", "SKU-1", "W2"); ok {
		t.Fatal("unexpected availability for W2")
	}
}

func TestSandboxRejectsNegativeAvailability(t *testing.T) {
	s := NewSandbox(nil)
	err := s.SetAvailability(context.Background(), AvailabilityUpdate{TenantID: "t1", SKUID: "SKU-1", WarehouseID: "W1", AvailableQty: -1})
	if err == nil {
		t.Fatal("want error for negative availability")
	}
}

func TestSandboxFailListForcesChannelDown(t *testing.T) {
	t.Setenv("TAPER_SANDBOX_FAIL_SKUS", "SKU-BAD, SKU-BAD2")
	s := NewSandbox(nil)
	err := s.SetAvailability(context.Background(), AvailabilityUpdate{TenantID: "t1", SKUID: "SKU-BAD", WarehouseID: "W1", AvailableQty: 1})
	if !errors.Is(err, ErrChannelDown) {
		t.Fatalf("want ErrChannelDown, got %v", err)
	}
	if err := s.SetAvailability(context.Background(), AvailabilityUpdate{TenantID: "t1", SKUID: "SKU-GOOD", WarehouseID: "W1", AvailableQty: 1}); err != nil {
		t.Fatalf("unlisted sku should succeed: %v", err)
	}
}

func TestSandboxNotifyDeficitRecords(t *testing.T) {
	s := NewSandbox(nil)
	if err := s.NotifyDeficit(context.Background(), DeficitAlert{TenantID: "t1", SKUID: "SKU-1", WarehouseID: "W1", Delta: -3, Reason: "external-sync", Source: "test"}); err != nil {
		t.Fatalf("NotifyDeficit: %v", err)
	}
	alerts := s.Alerts()
	if len(alerts) != 1 || alerts[0].Delta != -3 || alerts[0].SKUID != "SKU-1" {
		t.Fatalf("want one recorded alert, got %+v", alerts)
	}
}
