package main

import (
	"context"
	"log"
	"net"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	stockv1 "github.com/seifsheikhelarab/taper/gen/go/stock/v1"
	"github.com/seifsheikhelarab/taper/internal/stockservice"
	"github.com/seifsheikhelarab/taper/pkg/config"
)

func main() {
	addr := config.EnvOr("STOCK_ADDR", ":50051")
	dsn := config.EnvOr("STOCK_DATABASE_URL", "postgres://taper_app:taperapp@localhost:5432/taper_db")

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		log.Fatalf("connect db: %v", err)
	}
	defer pool.Close()

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer()
	stockv1.RegisterStockServiceServer(srv, stockservice.NewServer(pool))
	log.Printf("stock service listening on %s", addr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

