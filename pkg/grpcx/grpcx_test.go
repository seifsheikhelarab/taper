package grpcx

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
)

// backend is a real TCP gRPC server exposing the standard health service
// at a controllable status, so tests can tell which backend an RPC landed
// on by whether it succeeded.
type backend struct {
	addr string
	hs   *health.Server
	stop func()
}

func startBackend(t *testing.T, status healthv1.HealthCheckResponse_ServingStatus) *backend {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	hs := health.NewServer()
	hs.SetServingStatus("", status)
	healthv1.RegisterHealthServer(srv, hs)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve(lis)
	}()
	t.Cleanup(func() {
		srv.Stop()
		<-done
	})
	return &backend{addr: lis.Addr().String(), hs: hs}
}

// TestDialRoundRobinDistributesAcrossAddresses proves the service config
// installed by Dial spreads RPCs per-call across every resolved address:
// with one backend application-level NOT_SERVING and one SERVING, traffic
// lands on both (the not-serving backend answers Unavailable — blind
// per-call distribution, exactly the documented round_robin behavior).
// After the backend flips to SERVING, every check succeeds again.
func TestDialRoundRobinDistributesAcrossAddresses(t *testing.T) {
	a := startBackend(t, healthv1.HealthCheckResponse_SERVING)
	b := startBackend(t, healthv1.HealthCheckResponse_NOT_SERVING)

	mr := manual.NewBuilderWithScheme("taper-test")
	mr.InitialState(resolver.State{Addresses: []resolver.Address{
		{Addr: a.addr},
		{Addr: b.addr},
	}})

	conn, err := Dial(mr.Scheme()+":///cluster",
		grpc.WithResolvers(mr),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	cli := healthv1.NewHealthClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// WaitForReady queues each RPC until the channel is READY, so results
	// reflect per-call routing, not startup races. Health Check answers OK
	// even when the reported status is NOT_SERVING, so distribution is
	// observed through the response body.
	callOpts := []grpc.CallOption{grpc.WaitForReady(true)}

	phase1Serving, phase1Down := 0, 0
	for i := 0; i < 20; i++ {
		resp, err := cli.Check(ctx, &healthv1.HealthCheckRequest{}, callOpts...)
		if err != nil {
			t.Fatalf("check %d: %v", i, err)
		}
		switch resp.GetStatus() {
		case healthv1.HealthCheckResponse_SERVING:
			phase1Serving++
		case healthv1.HealthCheckResponse_NOT_SERVING:
			phase1Down++ // landed on backend b
		default:
			t.Fatalf("unexpected status: %v", resp.GetStatus())
		}
	}
	if phase1Serving == 0 || phase1Down == 0 {
		t.Fatalf("round_robin did not distribute: serving=%d not-serving=%d", phase1Serving, phase1Down)
	}

	// Recover the second backend; every subsequent answer is SERVING.
	b.hs.SetServingStatus("", healthv1.HealthCheckResponse_SERVING)
	for i := 0; i < 20; i++ {
		resp, err := cli.Check(ctx, &healthv1.HealthCheckRequest{}, callOpts...)
		if err != nil {
			t.Fatalf("check %d after recovery: %v", i, err)
		}
		if resp.GetStatus() != healthv1.HealthCheckResponse_SERVING {
			t.Fatalf("check %d after recovery: got %v", i, resp.GetStatus())
		}
	}
}
