# Observability Runbook

Phase 5 (spec #44). Tracing and metrics are wired into every `cmd/*` binary
via `pkg/observability`; no behavior changes when observability is off.

## Tracing

- **Backend**: Jaeger all-in-one (`docker compose up -d jaeger`) — UI on
  <http://localhost:16686>, OTLP gRPC on `4317`.
- **Enable export**: set `OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4317`
  (standard `http://localhost:4317` form also accepted). Without the
  variable services run never-sampled but still **propagate** valid trace
  context, so enabling one service does not orphan downstream traces.
- **Propagation paths**:
  - gRPC hops: W3C `traceparent` in gRPC metadata via the interceptors.
  - Kafka events: producers store the active traceparent in the outbox
    `traceparent` column; Debezium's EventRouter promotes it to a Kafka
    header (`additional.placement`); `pkg/streaming.Run` opens a consumer
    span joined to the producer trace. The traceparent survives the DLQ
    envelope and `dlqreplay` re-publishes it.
- **Verify end to end**:

  ```bash
  docker compose up -d jaeger
  JAEGER_URL=http://localhost:16686 go test -run TestSagaEndsUpAsOneConnectedTrace ./integration/
  ```

  One `CreateOrder` saga must appear as a single trace with spans from
  `order`, `reservation`, and `stock`.

## Metrics

Every binary serves Prometheus text format on a dedicated admin port
(`METRICS_ADDR`, default `:9101`–`:9106` for
stock/reservation/order/fulfillment/channelsync/gateway):

- `taper_grpc_request_duration_seconds{method,code}` — RED latency histogram
- `taper_grpc_request_errors_total{method,code}` — RED error rate
- `taper_grpc_requests_in_flight{method}` — concurrent requests
- `taper_breaker_state{dependency}` — 0 closed / 1 half-open / 2 open
  (Fail-Fast Policy visibility; published by the gateway on each
  transition)

Scrape example (prometheus.yml):

```yaml
scrape_configs:
  - job_name: taper
    static_configs:
      - targets: ["localhost:9101", "localhost:9102", "localhost:9103",
                  "localhost:9104", "localhost:9105", "localhost:9106"]
```

Quick check: `curl -s localhost:9101/metrics | grep taper_`.
