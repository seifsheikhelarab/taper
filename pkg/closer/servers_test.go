package closer

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
)

func testClientConn(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// DeferGRPC drains a live gRPC server: cleanup succeeds and subsequent
// health checks fail because the server stopped accepting.
func TestDeferGRPCStopsLiveServer(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	hs := health.NewServer()
	hs.SetServingStatus("", healthv1.HealthCheckResponse_SERVING)
	healthv1.RegisterHealthServer(srv, hs)
	go func() { _ = srv.Serve(lis) }()

	conn := testClientConn(t, lis.Addr().String())
	client := healthv1.NewHealthClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Check(ctx, &healthv1.HealthCheckRequest{}); err != nil {
		t.Fatalf("pre-drain check: %v", err)
	}

	fired := make(chan struct{})
	c := NewWithSignal(time.Second, fired)
	c.Defer(DeferGRPC(srv))
	close(fired)
	if code := c.Cleanup(); code != 0 {
		t.Fatalf("cleanup exit code: %d, want 0", code)
	}

	checkCtx, cancelCheck := context.WithTimeout(context.Background(), time.Second)
	defer cancelCheck()
	if _, err := client.Check(checkCtx, &healthv1.HealthCheckRequest{}); err == nil {
		t.Fatal("health check after shutdown succeeded, want failure (server stopped)")
	}
}

// DeferHTTP drains a live HTTP server: a slow in-flight request completes
// within the window and Shutdown reports success. The handler's completion
// must not depend on test-goroutine ordering that races the drain, so the
// handler closes over a release channel the test closes only after the
// drain window is proven long enough by the drain itself succeeding.
func TestDeferHTTPDrainsInFlightRequest(t *testing.T) {
	inFlight := make(chan struct{})
	release := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/slow", func(w http.ResponseWriter, _ *http.Request) {
		close(inFlight) // announce arrival; never blocks the handler
		select {
		case <-release:
			w.WriteHeader(http.StatusOK)
		case <-time.After(5 * time.Second):
			// Self-release so a test failure cannot leak goroutines forever.
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})
	srv := &http.Server{Handler: mux} //nolint:gosec // test server
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(lis) }()

	type result struct {
		status int
		err    error
	}
	done := make(chan result, 1)
	go func() {
		client := &http.Client{Timeout: 5 * time.Second}
		resp, err := client.Get("http://" + lis.Addr().String() + "/slow")
		if err != nil {
			done <- result{err: err}
			return
		}
		defer resp.Body.Close()
		done <- result{status: resp.StatusCode}
	}()

	// Wait until the request is genuinely inside the handler, then fire
	// the signal and release the handler while the drain window is open.
	select {
	case <-inFlight:
	case <-time.After(2 * time.Second):
		t.Fatal("request never reached the handler")
	}
	fired := make(chan struct{})
	c := NewWithSignal(2*time.Second, fired)
	c.Defer(DeferHTTP(srv))
	close(fired)
	close(release)
	code := c.Cleanup()

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("in-flight request failed: %v", r.err)
		}
		if r.status != http.StatusOK {
			t.Fatalf("in-flight request status: %d, want 200", r.status)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight request never completed")
	}
	if code != 0 {
		t.Fatalf("cleanup exit code: %d, want 0", code)
	}
}

// DeferHTTP force-closes when the drain window expires: the stuck request is
// cut and the error reports the exceeded window (exit code 1).
func TestDeferHTTPForceClosesWhenWindowExceeds(t *testing.T) {
	block := make(chan struct{})
	defer close(block)
	mux := http.NewServeMux()
	mux.HandleFunc("/stuck", func(http.ResponseWriter, *http.Request) {
		<-block // never released before the window expires
	})
	srv := &http.Server{Handler: mux} //nolint:gosec // test server
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(lis) }()
	go func() {
		client := &http.Client{Timeout: 5 * time.Second}
		resp, err := client.Get("http://" + lis.Addr().String() + "/stuck")
		if err == nil {
			_ = resp.Body.Close()
		}
	}()
	time.Sleep(100 * time.Millisecond)

	fired := make(chan struct{})
	c := NewWithSignal(50*time.Millisecond, fired)
	c.Defer(DeferHTTP(srv))
	close(fired)
	start := time.Now()
	code := c.Cleanup()
	if code != 1 {
		t.Fatalf("cleanup exit code: %d, want 1 when the drain window expires", code)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("force-close took %v, want well under the test ceiling", elapsed)
	}
}

// DeferGRPC force-stops when the window expires rather than hanging on an
// open server-side stream.
func TestDeferGRPCForceStopsWhenWindowExceeds(t *testing.T) {
	srv := grpc.NewServer()
	hs := health.NewServer()
	hs.SetServingStatus("", healthv1.HealthCheckResponse_SERVING)
	healthv1.RegisterHealthServer(srv, hs)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(lis) }()

	conn := testClientConn(t, lis.Addr().String())
	client := healthv1.NewHealthClient(conn)

	// Watch stays open server-side; GracefulStop waits on it until the
	// client stream closes, so the drain window must expire first.
	watchCtx, cancelWatch := context.WithCancel(context.Background())
	defer cancelWatch()
	if _, err := client.Watch(watchCtx, &healthv1.HealthCheckRequest{}); err != nil {
		t.Fatalf("watch: %v", err)
	}

	fired := make(chan struct{})
	c := NewWithSignal(50*time.Millisecond, fired)
	c.Defer(DeferGRPC(srv))
	close(fired)
	start := time.Now()
	code := c.Cleanup()
	if code != 1 {
		t.Fatalf("cleanup exit code: %d, want 1 when the drain window expires", code)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("force-stop took %v, want well under the test ceiling", elapsed)
	}
}
