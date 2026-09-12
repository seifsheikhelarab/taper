// Command fulfillment consumes order.events and drives stock fulfillment
// (Allocated -> Fulfilled) via the StockService gRPC.
package main

import (
	"context"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	stockv1 "github.com/seifsheikhelarab/taper/gen/go/stock/v1"
	"github.com/seifsheikhelarab/taper/internal/fulfillment"
	"github.com/seifsheikhelarab/taper/pkg/closer"
	"github.com/seifsheikhelarab/taper/pkg/config"
	"github.com/seifsheikhelarab/taper/pkg/grpcx"
	obs "github.com/seifsheikhelarab/taper/pkg/observability"
	"github.com/seifsheikhelarab/taper/pkg/streaming"
)

func main() {
	stockAddr := config.EnvOr("STOCK_ADDR", "localhost:50051")
	brokers := config.EnvOr("KAFKA_BROKERS", "localhost:29092")
	metricsAddr := config.EnvOr("METRICS_ADDR", ":9104")

	c := closer.New(closer.DrainWindow())

	provs, stopObs := obs.MustRun("fulfillment", metricsAddr)
	c.Defer(func(context.Context) error { stopObs(); return nil })

	// grpcx.Dial opts into client-side round_robin (docs/research/0002):
	// FulfillStock is idempotency-keyed, so per-call spreading across stock
	// replicas is safe.
	conn, err := grpcx.Dial(stockAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(provs.UnaryClientInterceptor()))
	if err != nil {
		log.Fatalf("dial stock: %v", err)
	}
	c.Defer(func(context.Context) error { _ = conn.Close(); return nil })

	svc := fulfillment.New(stockv1.NewStockServiceClient(conn), log.Printf)

	cons := streaming.NewConsumer(streaming.Config{
		Brokers: []string{brokers},
		Topic:   "order.events",
		GroupID: config.EnvOr("FULFILLMENT_GROUP_ID", "fulfillment"),
	}, log.Printf)

	log.Printf("fulfillment consuming order.events from %s", brokers)
	consumeErr := make(chan error, 1)
	go func() { consumeErr <- cons.Run(c.Context(), svc.HandleOrderEvent) }()

	// The consumer returns promptly on ctx cancel (FetchMessage honors it)
	// and closes its own reader; nothing further to drain.
	c.DeferFirst(func(context.Context) error { return <-consumeErr })

	osExit(c.Wait())
}

// osExit is a test seam over os.Exit: 0 returns normally (process exit 0),
// anything else is fatal.
var osExit = func(code int) {
	if code != 0 {
		log.Fatalf("fulfillment: unclean exit %d", code)
	}
}
