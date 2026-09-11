// Command fulfillment consumes order.events and drives stock fulfillment
// (Allocated -> Fulfilled) via the StockService gRPC.
package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	stockv1 "github.com/seifsheikhelarab/taper/gen/go/stock/v1"
	"github.com/seifsheikhelarab/taper/internal/fulfillment"
	"github.com/seifsheikhelarab/taper/pkg/config"
	obs "github.com/seifsheikhelarab/taper/pkg/observability"
	"github.com/seifsheikhelarab/taper/pkg/streaming"
)

func main() {
	stockAddr := config.EnvOr("STOCK_ADDR", "localhost:50051")
	brokers := config.EnvOr("KAFKA_BROKERS", "localhost:29092")
	metricsAddr := config.EnvOr("METRICS_ADDR", ":9104")

	provs, err := obs.Setup(context.Background(), obs.Config{
		ServiceName:  "fulfillment",
		OTLPEndpoint: config.EnvOr("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
	})
	if err != nil {
		log.Fatalf("observability setup: %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := provs.Shutdown(shutdownCtx); err != nil {
			log.Printf("observability shutdown: %v", err)
		}
	}()

	metricsSrv := obs.MetricsServer(provs.Metrics, metricsAddr)
	go func() {
		log.Printf("fulfillment metrics listening on %s", metricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics serve: %v", err)
		}
	}()
	defer metricsSrv.Close() //nolint:errcheck // admin endpoint at exit

	conn, err := grpc.NewClient(stockAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(provs.UnaryClientInterceptor()))
	if err != nil {
		log.Fatalf("dial stock: %v", err)
	}
	defer conn.Close()

	svc := fulfillment.New(stockv1.NewStockServiceClient(conn), log.Printf)

	c := streaming.NewConsumer(streaming.Config{
		Brokers: []string{brokers},
		Topic:   "order.events",
		GroupID: config.EnvOr("FULFILLMENT_GROUP_ID", "fulfillment"),
	}, log.Printf)
	log.Printf("fulfillment consuming order.events from %s", brokers)
	if err := c.Run(context.Background(), svc.HandleOrderEvent); err != nil {
		log.Fatalf("consumer: %v", err)
	}
}
