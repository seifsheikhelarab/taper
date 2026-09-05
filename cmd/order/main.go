package main

import (
	"context"
	"log"
	"net"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	orderv1 "github.com/seifsheikhelarab/taper/gen/go/order/v1"
	resv1 "github.com/seifsheikhelarab/taper/gen/go/reservation/v1"
	"github.com/seifsheikhelarab/taper/internal/orderservice"
	"github.com/seifsheikhelarab/taper/pkg/config"
	"github.com/seifsheikhelarab/taper/pkg/payment"
)

func main() {
	addr := config.EnvOr("ORDER_ADDR", ":50053")
	dsn := config.EnvOr("ORDER_DATABASE_URL", "postgres://taper_app:taperapp@localhost:5432/order_db")
	resAddr := config.EnvOr("RESERVATION_ADDR", "localhost:50052")

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		log.Fatalf("connect db: %v", err)
	}
	defer pool.Close()

	resConn, err := grpc.NewClient(resAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial reservation: %v", err)
	}
	defer resConn.Close()

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer()
	orderv1.RegisterOrderServiceServer(srv, orderservice.NewSaga(
		pool,
		resv1.NewReservationServiceClient(resConn),
		payment.NewSandbox(),
	))
	log.Printf("order service listening on %s", addr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
