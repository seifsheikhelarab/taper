// Server adapters for the shutdown harness: bounded drains for gRPC and HTTP
// servers plus a deadline-respecting Postgres ping for readiness checks.
package closer

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
)

// DeferGRPC registers a graceful stop that stops accepting new RPCs
// immediately, then drains in-flight calls. GracefulStop has no deadline, so
// the stop races the drain window: if the window expires first, Stop() force
// tears the server down so the process cannot hang past the bounded drain.
func DeferGRPC(srv *grpc.Server) CleanupFunc {
	return func(ctx context.Context) error {
		stopped := make(chan struct{})
		go func() {
			srv.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
			return nil
		case <-ctx.Done():
			srv.Stop()
			return fmt.Errorf("grpc drain exceeded window: %w", ctx.Err())
		}
	}
}

// DeferHTTP registers a graceful HTTP shutdown: listeners close immediately
// (no new conns), in-flight requests finish within the drain window, and
// expired idle connections are force-closed.
func DeferHTTP(srv *http.Server) CleanupFunc {
	return func(ctx context.Context) error {
		err := srv.Shutdown(ctx)
		if ctx.Err() != nil {
			_ = srv.Close()
			return fmt.Errorf("http drain exceeded window: %w", ctx.Err())
		}
		return err
	}
}

// Drain wires a blocking serve loop into the harness. Wire it as:
//
//	serveErr := make(chan error, 1)
//	go func() { serveErr <- srv.Serve(lis) }()
//	...
//	c.Defer(closer.Drain(closer.DeferGRPC(srv), func() error { return <-serveErr }))
//
// During cleanup, stop initiates the graceful drain (Serve/ListenAndServe
// returns as soon as shutdown begins, so reading run afterwards cannot
// deadlock), then run's error is joined in. http.ErrServerClosed and a nil
// Serve return are success; any other serve error is reported.
func Drain(stop CleanupFunc, run func() error) CleanupFunc {
	return func(ctx context.Context) error {
		stopErr := stop(ctx)
		runErr := run()
		if errors.Is(runErr, http.ErrServerClosed) {
			runErr = nil
		}
		return errors.Join(stopErr, runErr)
	}
}

// Ping returns a readiness probe closure for a pgxpool: pings with a bounded
// deadline so a hung database cannot hang the readiness handler.
func Ping(pool *pgxpool.Pool) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		return pool.Ping(pctx)
	}
}
