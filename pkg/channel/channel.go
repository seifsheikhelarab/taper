// Package channel provides the deterministic sandbox sales-channel adapter:
// it records availability upserts and routes deficit alerts for local dev
// and tests, until real platform integrations land (Shopify/WooCommerce
// adapters are out of scope for now).
package channel

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// ErrChannelDown is returned when the channel integration cannot accept an
// update. Consumers treat it as retryable (at-least-once redelivery).
var ErrChannelDown = errors.New("channel unavailable")

// AvailabilityUpdate is an absolute availability upsert for one SKU at one
// warehouse. Channels think in absolute sellable counts, never deltas
// (delta feeds are the dual-write smell ADR-0001 rejects).
type AvailabilityUpdate struct {
	TenantID     string
	SKUID        string
	WarehouseID  string
	AvailableQty int32
	UpdatedAt    time.Time
}

// DeficitAlert reports an External Sync Adjustment clamped to zero
// (CONTEXT.md: urgent deficit alert).
type DeficitAlert struct {
	TenantID    string
	SKUID       string
	WarehouseID string
	Delta       int32
	Reason      string
	Source      string
	DetectedAt  time.Time
}

// Sandbox is a deterministic in-process channel and notifier for local dev
// and tests. SetAvailability records the upsert; NotifyDeficit logs
// structurally and records the alert. Both fail when the SKU is listed in
// TAPER_SANDBOX_FAIL_SKUS (comma-separated) so integration tests can force
// ErrChannelDown via configuration.
type Sandbox struct {
	log func(format string, args ...any)

	mu           sync.Mutex
	availability map[string]AvailabilityUpdate // key tenant:sku:warehouse
	alerts       []DeficitAlert
}

// NewSandbox returns a Sandbox channel/notifier.
func NewSandbox(log func(format string, args ...any)) *Sandbox {
	if log == nil {
		log = func(string, ...any) {}
	}
	return &Sandbox{log: log, availability: map[string]AvailabilityUpdate{}}
}

// SetAvailability upserts the absolute sellable count for a SKU at a
// warehouse. Idempotent per (tenant, sku, warehouse); sandbox is
// deterministic in-process.
func (s *Sandbox) SetAvailability(_ context.Context, u AvailabilityUpdate) error {
	if u.AvailableQty < 0 {
		return fmt.Errorf("availability must be non-negative, got %d", u.AvailableQty)
	}
	if sandboxFailListed(u.SKUID) {
		return fmt.Errorf("%w: sku %s", ErrChannelDown, u.SKUID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.availability[availabilityKey(u.TenantID, u.SKUID, u.WarehouseID)] = u
	s.log("channel: availability tenant=%s sku=%s warehouse=%s qty=%d",
		u.TenantID, u.SKUID, u.WarehouseID, u.AvailableQty)
	return nil
}

// NotifyDeficit routes a clamped-adjustment deficit alert. Sandbox
// behavior: structured log plus in-memory record.
func (s *Sandbox) NotifyDeficit(_ context.Context, a DeficitAlert) error {
	if sandboxFailListed(a.SKUID) {
		return fmt.Errorf("%w: sku %s", ErrChannelDown, a.SKUID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.alerts = append(s.alerts, a)
	s.log("ALERT deficit tenant=%s sku=%s warehouse=%s delta=%d reason=%s source=%s",
		a.TenantID, a.SKUID, a.WarehouseID, a.Delta, a.Reason, a.Source)
	return nil
}

// Availability returns the last upserted availability for a location
// (inspection for tests and the sandbox UI).
func (s *Sandbox) Availability(tenant, sku, warehouse string) (AvailabilityUpdate, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.availability[availabilityKey(tenant, sku, warehouse)]
	return u, ok
}

// Alerts returns the recorded deficit alerts (inspection for tests).
func (s *Sandbox) Alerts() []DeficitAlert {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]DeficitAlert(nil), s.alerts...)
}

func availabilityKey(tenant, sku, warehouse string) string {
	return tenant + ":" + sku + ":" + warehouse
}

// sandboxFailListed checks TAPER_SANDBOX_FAIL_SKUS so integration tests can
// force ErrChannelDown via configuration (mirrors pkg/payment).
func sandboxFailListed(sku string) bool {
	v := os.Getenv("TAPER_SANDBOX_FAIL_SKUS")
	if v == "" {
		return false
	}
	for _, id := range strings.Split(v, ",") {
		if strings.TrimSpace(id) == sku {
			return true
		}
	}
	return false
}
