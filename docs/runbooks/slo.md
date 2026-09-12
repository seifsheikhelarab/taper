# SLO Definitions & Alerting (spec #52, US25)

Alert rules live in `deploy/monitoring.yaml` (PrometheusRule); this
document records the SLOs they enforce and the response expectations.

## SLOs (targets provisional — business sign-off pending, `ready-for-human`)

| SLO | Target | Window | Source metric |
|---|---|---|---|
| Gateway availability | 99.5% non-5xx | rolling 30d | `taper_grpc_request_errors_total` / `taper_grpc_request_duration_seconds_count` |
| Gateway latency | p99 < 1s | rolling 30d | `taper_grpc_request_duration_seconds_bucket` |
| Saga success | ≥ 95% of CreateOrder reach CONFIRMED | rolling 30d | `taper_grpc_request_duration_seconds_count{method=~".*CreateOrder.*"}` |
| Readiness integrity | no dependency-unready > 2m outside maintenance | rolling 7d | `up` / `/readyz` |

Error budget policy: when 50% of the monthly budget burns inside 1/4 of the
window, freeze non-essential deploys until burn drops below the line (page:
`TaperGatewayHighErrorRate`).

## Alert rules → response

| Alert | Meaning | First response |
|---|---|---|
| `TaperGatewayHighErrorRate` (critical) | budget burning fast | incident runbook, first-5-minutes |
| `TaperGatewayHighLatency` (warning) | p99 > 1s sustained | check downstream `taper_grpc_request_duration_seconds` per method; find the slow dep |
| `TaperBreakerOpen` (critical) | Fail-Fast Policy open on a dependency | see dependency-down play |
| `TaperSagaSuccessLow` (warning) | sagas failing/compensating | check payment sandbox + reservation stock levels |
| `TaperServiceNotReady` (warning) | readiness failing | dependency down or draining — verify intent before restarting |

Structured log lines (`log/slog` JSON, stdout) carry `trace_id` on every
request-scoped record: paste it into Jaeger to jump to the distributed
trace.
