package main

import (
	"context"
	"log"
	"net"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	resv1 "github.com/seifsheikhelarab/taper/gen/go/reservation/v1"
	stockv1 "github.com/seifsheikhelarab/taper/gen/go/stock/v1"
	"github.com/seifsheikhelarab/taper/internal/reservationservice"
)

func main() {
	ctx := context.Background()
	addr := envOr("RESERVATION_ADDR", ":50052")
	dsn := envOr("DATABASE_URL", "postgres://postgres:postgrespassword@localhost:5432/taper_db")
	stockAddr := envOr("STOCK_ADDR", ":50051")

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("connect db: %v", err)
	}
	defer pool.Close()

	conn, err := grpc.NewClient(stockAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial stock: %v", err)
	}
	defer conn.Close()
	stock := stockv1.NewStockServiceClient(conn)

	srv := grpc.NewServer()
	resv1.RegisterReservationServiceServer(srv, reservationservice.NewServer(pool, stock))

	sweeper := reservationservice.NewSweeper(pool, stock)
	go sweeper.Run(ctx)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("reservation service listening on %s", addr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
