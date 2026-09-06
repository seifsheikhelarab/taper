// Package fulfillment reacts to order.fulfilled events by transitioning the
// order's stock from Allocated to Fulfilled via the StockService gRPC.
package fulfillment

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"

	stockv1 "github.com/seifsheikhelarab/taper/gen/go/stock/v1"
)

// Service is the fulfillment reactor.
type Service struct {
	stock stockv1.StockServiceClient
	log   func(format string, args ...any)
}

// New builds the fulfillment reactor.
func New(stock stockv1.StockServiceClient, log func(format string, args ...any)) *Service {
	if log == nil {
		log = func(string, ...any) {}
	}
	return &Service{stock: stock, log: log}
}

// HandleOrderEvent processes one order.events message. Only
// order.fulfilled is acted on; other order events are ignored so a single
// consumer group can subscribe to the whole topic.
func (s *Service) HandleOrderEvent(ctx context.Context, msg kafka.Message) error {
	var ev struct {
		EventType string `json:"event_type"`
		TenantID  string `json:"tenant_id"`
		OrderID   string `json:"order_id"`
		Lines     []struct {
			SkuID       string `json:"sku_id"`
			WarehouseID string `json:"warehouse_id"`
			Quantity    int32  `json:"quantity"`
		} `json:"lines"`
	}
	if err := json.Unmarshal(msg.Value, &ev); err != nil {
		return fmt.Errorf("decode order event: %w", err)
	}
	if ev.EventType != "order.fulfilled" {
		return nil
	}
	if ev.TenantID == "" || ev.OrderID == "" {
		// Fall back to the composite partition key tenant:order.
		for i := 0; i < len(msg.Key); i++ {
			if msg.Key[i] == ':' {
				ev.TenantID, ev.OrderID = string(msg.Key[:i]), string(msg.Key[i+1:])
				break
			}
		}
	}
	if ev.TenantID == "" || ev.OrderID == "" || len(ev.Lines) == 0 {
		return fmt.Errorf("order.fulfilled missing tenant/order/lines (key=%s)", string(msg.Key))
	}

	// A single FulfillStock call covers the whole order: the stock handler is
	// idempotent (same idempotency key) and rejects replays atomically.
	lines := make([]*stockv1.StockLine, 0, len(ev.Lines))
	for _, l := range ev.Lines {
		lines = append(lines, &stockv1.StockLine{
			SkuId:       l.SkuID,
			WarehouseId: l.WarehouseID,
			Quantity:    l.Quantity,
		})
	}
	resp, err := s.stock.FulfillStock(ctx, &stockv1.FulfillStockRequest{
		TenantId:       ev.TenantID,
		OrderId:        ev.OrderID,
		IdempotencyKey: "fulfill:" + ev.TenantID + ":" + ev.OrderID,
		Lines:          lines,
	})
	if err != nil {
		return fmt.Errorf("FulfillStock %s: %w", ev.OrderID, err)
	}
	s.log("fulfillment: order=%s stock fulfilled (%d lines)", ev.OrderID, len(resp.GetLines()))
	return nil
}
