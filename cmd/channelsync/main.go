// Command channelsync consumes stock.events and pushes absolute availability
// upserts to external sales channels, routing deficit alerts. Sandbox
// adapters are used until real platform integrations land (out of scope).
package main

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/seifsheikhelarab/taper/internal/channelsync"
	"github.com/seifsheikhelarab/taper/pkg/channel"
	"github.com/seifsheikhelarab/taper/pkg/config"
	"github.com/seifsheikhelarab/taper/pkg/streaming"
)

func main() {
	dsn := config.EnvOr("CHANNELSYNC_DATABASE_URL", "postgres://taper_app:taperapp@localhost:5432/channelsync_db")
	brokers := config.EnvOr("KAFKA_BROKERS", "localhost:29092")

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		log.Fatalf("connect db: %v", err)
	}
	defer pool.Close()

	channels := channel.NewSandbox(log.Printf)
	notifier := channel.NewSandbox(log.Printf)
	svc := channelsync.New(pool, channels, notifier, log.Printf)

	c := streaming.NewConsumer(streaming.Config{
		Brokers: []string{brokers},
		Topic:   "stock.events",
		GroupID: config.EnvOr("CHANNELSYNC_GROUP_ID", "channelsync"),
	}, log.Printf)
	log.Printf("channelsync consuming stock.events from %s", brokers)
	if err := c.Run(context.Background(), svc.HandleStockEvent); err != nil {
		log.Fatalf("consumer: %v", err)
	}
}
