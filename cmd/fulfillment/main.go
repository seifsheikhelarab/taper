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
	"github.com/seifsheikhelarab/taper/pkg/config"
	"github.com/seifsheikhelarab/taper/pkg/streaming"
)

func main() {
	stockAddr := config.EnvOr("STOCK_ADDR", "localhost:50051")
	brokers := config.EnvOr("KAFKA_BROKERS", "localhost:29092")

	conn, err := grpc.NewClient(stockAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
