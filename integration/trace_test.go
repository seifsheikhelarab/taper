package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	orderv1 "github.com/seifsheikhelarab/taper/gen/go/order/v1"
)

// Phase 5 (spec #44, T3): one saga through real service subprocesses with
// OTLP export must appear in Jaeger as a single connected trace spanning
// order -> reservation -> stock. Skipped unless the compose Jaeger is up
// (JAEGER_URL set), keeping plain CI green; runbook:
// docs/runbooks/observability.md.
//
// Trace attribution: only this test's subprocess stack exports OTLP, so any
// CreateOrder trace newer than the test start belongs to this run (the
// bufconn harness exports nothing). Traces older than the start window are
// from previous runs and are ignored.

type jaegerSpan struct {
	TraceID       string `json:"traceID"`
	ProcessID     string `json:"processID"`
	OperationName string `json:"operationName"`
	StartTime     int64  `json:"startTime"` // microseconds since epoch
}

type jaegerTrace struct {
	Spans []jaegerSpan `json:"spans"`
	// Jaeger maps spans to services via a processID -> process map.
	Processes map[string]struct {
		ServiceName string `json:"serviceName"`
	} `json:"processes"`
}

type jaegerSearchResponse struct {
	Data []jaegerTrace `json:"data"`
}

// goBuild builds a service binary into a temp dir and returns its path.
func goBuild(t *testing.T, pkg string) string {
	t.Helper()
	name := filepath.Base(pkg)
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	bin := filepath.Join(t.TempDir(), name)
	cmd := exec.Command("go", "build", "-o", bin, pkg)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", pkg, err, out)
	}
	return bin
}

// startService launches a service subprocess with OTLP export pointed at
// the compose Jaeger. kv is flat key/value pairs ("KEY", "value", ...);
// they are joined into proper KEY=value entries for os/exec.
func startService(t *testing.T, bin string, kv ...string) *exec.Cmd {
	t.Helper()
	if len(kv)%2 != 0 {
		t.Fatalf("startService: odd number of key/value args for %s", bin)
	}
	env := os.Environ()
	for i := 0; i+1 < len(kv); i += 2 {
		env = append(env, kv[i]+"="+kv[i+1])
	}
	cmd := exec.Command(bin)
	cmd.Env = env
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", bin, err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})
	return cmd
}

// requireFreePort fails fast with an actionable message when a local
// service already occupies a port the subprocesses need.
func requireFreePort(t *testing.T, port string) {
	t.Helper()
	l, err := net.Listen("tcp", "localhost:"+port)
	if err != nil {
		t.Fatalf("port %s already in use — stop locally running taper services before this test: %v", port, err)
	}
	_ = l.Close()
}

func TestSagaEndsUpAsOneConnectedTrace(t *testing.T) {
	jaegerURL := os.Getenv("JAEGER_URL")
	if jaegerURL == "" {
		t.Skip("JAEGER_URL not set; skipping Jaeger-gated trace test")
	}
	otlpEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if otlpEndpoint == "" {
		otlpEndpoint = "localhost:4317"
	}
	for _, p := range []string{"50051", "50052", "50053"} {
		requireFreePort(t, p)
	}
	testStartUs := time.Now().UnixMicro()

	stockBin := goBuild(t, "../cmd/stock")
	resBin := goBuild(t, "../cmd/reservation")
	orderBin := goBuild(t, "../cmd/order")

	// DSNs target the compose postgres on host port 5433 (secondary
	// mapping) so a locally installed Postgres on 5432 cannot shadow the
	// container's databases.
	const pg = "localhost:5433"
	startService(t, stockBin,
		"STOCK_ADDR", "localhost:50051",
		"STOCK_DATABASE_URL", "postgres://taper_app:taperapp@"+pg+"/taper_db",
		"STOCK_SWEEPER_DATABASE_URL", "postgres://taper_sweeper:tapersweeper@"+pg+"/taper_db",
		"OTEL_EXPORTER_OTLP_ENDPOINT", otlpEndpoint,
	)
	startService(t, resBin,
		"RESERVATION_ADDR", "localhost:50052",
		"STOCK_ADDR", "localhost:50051",
		"RESERVATION_DATABASE_URL", "postgres://taper_app:taperapp@"+pg+"/reservation_db",
		"SWEEPER_DATABASE_URL", "postgres://taper_sweeper:tapersweeper@"+pg+"/reservation_db",
		"OTEL_EXPORTER_OTLP_ENDPOINT", otlpEndpoint,
	)
	startService(t, orderBin,
		"ORDER_ADDR", "localhost:50053",
		"RESERVATION_ADDR", "localhost:50052",
		"STOCK_ADDR", "localhost:50051",
		"ORDER_DATABASE_URL", "postgres://taper_app:taperapp@"+pg+"/order_db",
		"ORDER_SWEEPER_DATABASE_URL", "postgres://taper_sweeper:tapersweeper@"+pg+"/order_db",
		"OTEL_EXPORTER_OTLP_ENDPOINT", otlpEndpoint,
	)
	time.Sleep(1500 * time.Millisecond) // let the three listeners bind

	conn, err := grpc.NewClient("localhost:50053", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial order: %v", err)
	}
	defer conn.Close()
	order := orderv1.NewOrderServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = order.CreateOrder(ctx, &orderv1.CreateOrderRequest{
		TenantId: "11111111-1111-4111-8111-111111111111",
		OrderId:  "trace-ord-" + fmt.Sprint(testStartUs%1_000_000),
		Lines: []*orderv1.OrderLine{{
			SkuId:       "TRACE-SKU",
			WarehouseId: "W1",
			Quantity:    1,
			UnitPrice:   100,
		}},
	})
	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}

	// Poll Jaeger for a CreateOrder trace from this run's start window and
	// assert its spans cover order, reservation, and stock.
	deadline := time.Now().Add(60 * time.Second)
	var found *jaegerTrace
	for time.Now().Before(deadline) {
		for _, tr := range jaegerSearch(t, jaegerURL, "order.v1.OrderService/CreateOrder") {
			for _, s := range tr.Spans {
				if s.OperationName == "order.v1.OrderService/CreateOrder" && s.StartTime >= testStartUs {
					candidate := tr
					found = &candidate
					break
				}
			}
			if found != nil {
				break
			}
		}
		if found != nil {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if found == nil {
		t.Fatal("no saga trace from this run reached Jaeger within 60s (is the OTLP exporter reaching localhost:4317?)")
	}

	services := map[string]bool{}
	for _, s := range found.Spans {
		if p, ok := found.Processes[s.ProcessID]; ok {
			services[p.ServiceName] = true
		}
	}
	for _, svc := range []string{"order", "reservation", "stock"} {
		if !services[svc] {
			t.Errorf("trace missing %s service spans (have %v)", svc, services)
		}
	}
}

// jaegerSearch queries the Jaeger API for the order service's traces
// containing an operation (service is a required query parameter).
func jaegerSearch(t *testing.T, base, operation string) []jaegerTrace {
	t.Helper()
	url := fmt.Sprintf("%s/api/traces?service=order&operation=%s&lookback=15m&limit=20", base, operation)
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	var parsed jaegerSearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil
	}
	return parsed.Data
}
