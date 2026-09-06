package channelsync

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	channelsyncdb "github.com/seifsheikhelarab/taper/gen/go/db/channelsync"
	"github.com/seifsheikhelarab/taper/pkg/channel"
	"github.com/seifsheikhelarab/taper/pkg/database"
)

// Service reacts to stock.events: it maintains the absolute availability
// projection and routes deficit alerts (spec #24 US1/US2). Handlers are
// idempotent: each event carries an id (Debezium places the outbox row id in
// the "id" header), recorded in processed_idempotency_keys within the same
// transaction as the projection write, so redelivery is a no-op.
type Service struct {
	pool     *pgxpool.Pool
	channels channel.ChannelGateway
	notifier channel.NotificationGateway
	log      func(format string, args ...any)
}

// New builds the channelsync reactor.
func New(pool *pgxpool.Pool, channels channel.ChannelGateway, notifier channel.NotificationGateway, log func(format string, args ...any)) *Service {
	if log == nil {
		log = func(string, ...any) {}
	}
	return &Service{pool: pool, channels: channels, notifier: notifier, log: log}
}

// HandleStockEvent processes one stock.events message. Errors are retryable
// (at-least-once redelivery).
func (s *Service) HandleStockEvent(ctx context.Context, msg kafka.Message) error {
	eventID := headerValue(msg.Headers, "id")
	if eventID == "" {
		// No id header: fall back to a content-derived key so processing
		// still dedupes redeliveries of the same message.
		eventID = fmt.Sprintf("raw:%s:%s:%d", msg.Topic, string(msg.Key), msg.Offset)
	}
	eventType := headerValue(msg.Headers, "eventType")
	if eventType == "" {
		var probe struct {
			EventType string `json:"event_type"`
		}
		_ = json.Unmarshal(msg.Value, &probe)
		eventType = probe.EventType
	}

	tenant, sku, ok := splitKey(string(msg.Key))
	if !ok {
		s.log("channelsync: skip malformed key %q", string(msg.Key))
		return nil
	}

	switch eventType {
	case "stock.adjusted":
		return s.handleAdjusted(ctx, eventID, tenant, sku, msg.Value)
	case "stock.deficit_alert":
		return s.handleDeficit(ctx, eventID, tenant, sku, msg.Value)
	case "stock.reserved", "stock.released", "stock.allocated", "stock.fulfilled":
		// Line events carry order-level quantities, not availability
		// changes; the adjusting event already moved availability.
		return nil
	default:
		s.log("channelsync: ignore event type %q", eventType)
		return nil
	}
}

// handleAdjusted upserts the projection from the event's absolute
// available_qty and pushes it to the channel.
func (s *Service) handleAdjusted(ctx context.Context, eventID, tenant, sku string, value []byte) error {
	var ev struct {
		SKUID         string `json:"sku_id"`
		WarehouseID   string `json:"warehouse_id"`
		QuantityDelta int32  `json:"quantity_delta"`
		AvailableQty  int32  `json:"available_qty"`
	}
	if err := json.Unmarshal(value, &ev); err != nil {
		return fmt.Errorf("decode stock.adjusted: %w", err)
	}
	warehouse := ev.WarehouseID
	if warehouse == "" {
		return fmt.Errorf("stock.adjusted missing warehouse_id")
	}

	tenantUUID, err := database.ParseUUID(tenant)
	if err != nil {
		return fmt.Errorf("tenant id: %w", err)
	}
	// The event carries the tenant; RLS-scoped writes need it in context.
	ctx = database.WithTenantID(ctx, tenant)
	// Push first, then commit the projection/idempotency row: a crash
	// between the two replays the event, and the idempotent upsert makes
	// the re-push harmless.
	if err := s.channels.SetAvailability(ctx, channel.AvailabilityUpdate{
		TenantID:     tenant,
		SKUID:        sku,
		WarehouseID:  warehouse,
		AvailableQty: ev.AvailableQty,
	}); err != nil {
		return err
	}
	return database.ExecTxWithTenant(ctx, s.pool, func(tx pgx.Tx) error {
		q := channelsyncdb.New(tx)
		dup, err := tryIdempotency(ctx, q, tenantUUID, eventID)
		if err != nil {
			return err
		}
		if dup {
			return nil
		}
		if _, err := q.UpsertAvailability(ctx, channelsyncdb.UpsertAvailabilityParams{
			TenantID:     tenantUUID,
			SkuID:        sku,
			WarehouseID:  warehouse,
			AvailableQty: ev.AvailableQty,
		}); err != nil {
			return err
		}
		return nil
	})
}

// handleDeficit persists the alert and sends it through the notifier.
func (s *Service) handleDeficit(ctx context.Context, eventID, tenant, sku string, value []byte) error {
	var ev struct {
		SKUID       string `json:"sku_id"`
		WarehouseID string `json:"warehouse_id"`
		Delta       int32  `json:"delta"`
		Reason      string `json:"reason"`
		Source      string `json:"source"`
	}
	if err := json.Unmarshal(value, &ev); err != nil {
		return fmt.Errorf("decode stock.deficit_alert: %w", err)
	}
	warehouse := ev.WarehouseID
	if warehouse == "" {
		return fmt.Errorf("stock.deficit_alert missing warehouse_id")
	}
	tenantUUID, err := database.ParseUUID(tenant)
	if err != nil {
		return fmt.Errorf("tenant id: %w", err)
	}
	ctx = database.WithTenantID(ctx, tenant)
	if err := s.notifier.NotifyDeficit(ctx, channel.DeficitAlert{
		TenantID:    tenant,
		SKUID:       sku,
		WarehouseID: warehouse,
		Delta:       ev.Delta,
		Reason:      ev.Reason,
		Source:      ev.Source,
	}); err != nil {
		return err
	}
	return database.ExecTxWithTenant(ctx, s.pool, func(tx pgx.Tx) error {
		q := channelsyncdb.New(tx)
		dup, err := tryIdempotency(ctx, q, tenantUUID, eventID)
		if err != nil {
			return err
		}
		if dup {
			return nil
		}
		_, err = q.InsertAlert(ctx, channelsyncdb.InsertAlertParams{
			TenantID:    tenantUUID,
			SkuID:       sku,
			WarehouseID: warehouse,
			Delta:       ev.Delta,
			Reason:      ev.Reason,
			Source:      ev.Source,
		})
		return err
	})
}

// tryIdempotency returns true when eventID was already processed.
func tryIdempotency(ctx context.Context, q *channelsyncdb.Queries, tenantUUID pgtype.UUID, eventID string) (bool, error) {
	if _, err := q.CheckAndInsertIdempotencyKey(ctx, channelsyncdb.CheckAndInsertIdempotencyKeyParams{
		TenantID:       tenantUUID,
		IdempotencyKey: eventID,
	}); err == pgx.ErrNoRows {
		return true, nil // conflict: already processed
	} else if err != nil {
		return false, err
	}
	return false, nil
}

func headerValue(headers []kafka.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// splitKey splits the composite partition key "tenant:sku" (the outbox
// aggregate_id). Per-location values come from the payload.
func splitKey(key string) (tenant, rest string, ok bool) {
	i := strings.IndexByte(key, ':')
	if i <= 0 || i == len(key)-1 {
		return "", "", false
	}
	return key[:i], key[i+1:], true
}
