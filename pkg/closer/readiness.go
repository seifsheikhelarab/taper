package closer

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// probeTimeout bounds each individual dependency check so one hung
// dependency cannot stretch the readiness probe past its handler budget.
const probeTimeout = 2 * time.Second

// Readiness aggregates named dependency checks: it returns nil (ready) only
// when every check succeeds, otherwise the first failure. Checks run
// concurrently so one hung dependency cannot stretch the probe.
func Readiness(checks map[string]func(context.Context) error) func(context.Context) error {
	return func(ctx context.Context) error {
		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			firstEr string
		)
		for name, check := range checks {
			wg.Add(1)
			go func(name string, check func(context.Context) error) {
				defer wg.Done()
				if err := check(ctx); err != nil {
					mu.Lock()
					if firstEr == "" {
						firstEr = fmt.Sprintf("%s: %v", name, err)
					}
					mu.Unlock()
				}
			}(name, check)
		}
		wg.Wait()
		if firstEr != "" {
			return fmt.Errorf("%s", firstEr)
		}
		return nil
	}
}

// TCPDial returns a readiness check that dials addr (host:port) and closes
// the connection: the Kafka connectivity check for consumers, and a generic
// reachability probe for TCP dependencies.
func TCPDial(addr string) func(context.Context) error {
	return func(ctx context.Context) error {
		d := net.Dialer{}
		conn, err := d.DialContext(ctx, "tcp", addr)
		if err != nil {
			return err
		}
		return conn.Close()
	}
}

// KafkaBrokers parses a comma-separated KAFKA_BROKERS value into addresses.
func KafkaBrokers(v string) []string {
	var addrs []string
	for _, a := range strings.Split(v, ",") {
		if a = strings.TrimSpace(a); a != "" {
			addrs = append(addrs, a)
		}
	}
	return addrs
}

// GRPCHealth returns a readiness check for a dialed gRPC connection using
// the standard gRPC health protocol.
func GRPCHealth(conn *grpc.ClientConn) func(context.Context) error {
	return func(ctx context.Context) error {
		pctx, cancel := context.WithTimeout(ctx, probeTimeout)
		defer cancel()
		resp, err := healthv1.NewHealthClient(conn).Check(pctx, &healthv1.HealthCheckRequest{})
		if err != nil {
			return err
		}
		if resp.GetStatus() != healthv1.HealthCheckResponse_SERVING {
			return fmt.Errorf("health status %s", resp.GetStatus())
		}
		return nil
	}
}

// GRPCReach is the lighter check for servers that do not register the gRPC
// health service (the taper gRPC binaries currently do not): the probe RPC
// returns Unimplemented when the server is up and answers, which counts as
// reachable. Connection-level failures (refused, deadline) do not.
func GRPCReach(conn *grpc.ClientConn) func(context.Context) error {
	return func(ctx context.Context) error {
		pctx, cancel := context.WithTimeout(ctx, probeTimeout)
		defer cancel()
		err := conn.Invoke(pctx, "/grpc.health.v1.Health/Check",
			&healthv1.HealthCheckRequest{}, &healthv1.HealthCheckResponse{})
		if err == nil {
			return nil
		}
		if status.Code(err) == codes.Unimplemented {
			return nil
		}
		return err
	}
}
