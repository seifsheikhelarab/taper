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
	"github.com/seifsheikhelarab/taper/pkg/closer"
	"github.com/seifsheikhelarab/taper/pkg/config"
	obs "github.com/seifsheikhelarab/taper/pkg/observability"
	"github.com/seifsheikhelarab/taper/pkg/streaming"
)

func main() {
	dsn := config.EnvOr("CHANNELSYNC_DATABASE_URL", "postgres://taper_app:taperapp@localhost:5432/channelsync_db")
	brokers := config.EnvOr("KAFKA_BROKERS", "localhost:29092")
	metricsAddr := config.EnvOr("METRICS_ADDR", ":9105")

	c := closer.New(closer.DrainWindow())

	_, stopObs := obs.MustRun("channelsync", metricsAddr)
	c.Defer(func(context.Context) error { stopObs(); return nil })

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		log.Fatalf("connect db: %v", err)
	}
	c.Defer(func(context.Context) error { pool.Close(); return nil })

	channels := channel.NewSandbox(log.Printf)
	notifier := channel.NewSandbox(log.Printf)
	svc := channelsync.New(pool, channels, notifier, log.Printf)

	cons := streaming.NewConsumer(streaming.Config{
		Brokers: []string{brokers},
		Topic:   "stock.events",
		GroupID: config.EnvOr("CHANNELSYNC_GROUP_ID", "channelsync"),
	}, log.Printf)

	log.Printf("channelsync consuming stock.events from %s", brokers)
	consumeErr := make(chan error, 1)
	go func() { consumeErr <- cons.Run(c.Context(), svc.HandleStockEvent) }()

	// The consumer returns promptly on ctx cancel and closes its own
	// reader; nothing further to drain.
	c.DeferFirst(func(context.Context) error { return <-consumeErr })

	osExit(c.Wait())
}

// osExit is a test seam over os.Exit: 0 returns normally (process exit 0),
// anything else is fatal.
var osExit = func(code int) {
	if code != 0 {
		log.Fatalf("channelsync: unclean exit %d", code)
	}
}
