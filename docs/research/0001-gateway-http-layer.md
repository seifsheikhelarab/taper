# Gateway HTTP layer: grpc-gateway vs hand-rolled vs Connect

**Question.** For the Phase 4 API gateway (REST fronting the internal
grpc-go services, JWT tenant resolution, rate limiting, Idempotency-Key
passthrough, breaker → 503 mapping): should the HTTP layer be generated
with grpc-gateway, hand-rolled with net/http, or served via Connect?

**Researched**: 2026-09-07, against primary sources only. Repo state
cross-checked against `go.mod` and `buf.gen.yaml`.

---

## Option A — grpc-gateway (proto-annotation codegen)

- REST routes are declared as `google.api.http` annotations in the
  `.proto` files; `protoc-gen-grpc-gateway` generates a reverse-proxy that
  maps HTTP/JSON ↔ gRPC. ([grpc-gateway docs](https://github.com/grpc-ecosystem/grpc-gateway))
- The generated `runtime.ServeMux` is a standard `http.Handler`, so JWT,
  rate-limiting, and future OTel/Prometheus middleware wrap it normally.
  ([FAQ](https://grpc-ecosystem.github.io/grpc-gateway/docs/faq/))
- Error translation is built in: `HTTPStatusFromCode` in
  [`runtime/errors.go`](https://github.com/grpc-ecosystem/grpc-gateway/blob/main/runtime/errors.go)
  maps gRPC codes to HTTP statuses (e.g. `invalid_argument` → 400,
  `failed_precondition` → 412, `unavailable` → 503 — which matches the
  gateway's Fail-Fast 503 requirement without custom mapping).
  ([runtime docs](https://pkg.go.dev/github.com/grpc-ecosystem/grpc-gateway/v2/runtime))
- OpenAPI v2 can be generated from the same annotations
  (`protoc-gen-openapiv2`). ([FAQ](https://grpc-ecosystem.github.io/grpc-gateway/docs/faq/))
- Costs, from the same sources:
  - The gateway "is intended to cover 80% of use cases"; arbitrary custom
    mappings need middleware anyway. ([FAQ](https://grpc-ecosystem.github.io/grpc-gateway/docs/faq/))
  - Proto changes require regenerating the proxy (same regen loop the repo
    already runs via `buf generate`). ([FAQ](https://grpc-ecosystem.github.io/grpc-gateway/docs/faq/))
  - JSON↔protobuf re-parsing at the proxy adds overhead vs binary passthrough.
    ([FAQ](https://grpc-ecosystem.github.io/grpc-gateway/docs/faq/))
- **Repo fit**: `go.mod` has zero gateway dependencies today;
  `buf.gen.yaml` (v2) has exactly two plugins. Adopting grpc-gateway adds a
  third plugin plus a `googleapis` dependency (annotations.proto) to the
  buf pipeline, and `google.api.http` annotations to every RPC that gets a
  REST route. The codegen step slots into the existing `buf generate` flow.

## Option B — hand-rolled net/http handlers

- The gateway defines REST routes directly (stdlib or chi — the spec
  originally named chi for Go services; [idea.md §8](../../idea.md)) and
  calls the already-generated grpc-go clients. No new codegen, no proto
  annotations, no `googleapis` dependency.
- Error mapping must be written once locally — the same gRPC→HTTP table
  that [`runtime/errors.go`](https://github.com/grpc-ecosystem/grpc-gateway/blob/main/runtime/errors.go)
  implements (`HTTPStatusFromCode`); it is a small, stable function.
- REST shapes, docs, and proto stay in lockstep only by discipline; there
  is no generated OpenAPI.
- **Repo fit**: zero new dependencies beyond a router; every REST decision
  (paths, status codes, body shape) is explicit Go code. This is the
  "deliberate manual SQL instead of ORM" style the repo already uses
  ([ADR-0001](../adr/0001-architecture-foundations.md) row-locking choice,
  sqlc over ORM).

## Option C — Connect (connectrpc.com)

- Connect handlers serve gRPC, gRPC-Web, and Connect protocols from one
  handler; error codes "use the same names and have the same semantics" as
  gRPC status codes. ([connect-go](https://github.com/connectrpc/connect-go),
  [errors docs](https://connectrpc.com/docs/go/errors/))
- For unary RPCs the Connect protocol returns real 4xx/5xx HTTP statuses
  with a JSON body `{"code": "...", "message": "..."}` — friendlier to REST
  clients than gRPC's HTTP-200-with-trailers error shape.
  ([errors docs](https://connectrpc.com/docs/go/errors/))
- **Repo fit**: the repo's five services are grpc-go servers with grpc-go
  stubs; Connect serving would mean swapping handlers per service
  (`protoc-gen-connect-go`) — a cross-service migration, not a gateway-only
  change. Connect-Go interceptors also do not apply to plain gRPC handlers
  ([FAQ](https://connectrpc.com/docs/faq/)), so mixed deployments carry
  caveats. This is the largest blast radius of the three options.

## Tradeoff summary

| Concern | A: grpc-gateway | B: hand-rolled | C: Connect |
|---|---|---|---|
| REST/proto lockstep | generated from annotations | manual discipline | generated, but new server type |
| New dependencies | buf plugin + googleapis dep | router only | protoc plugin + connect-go in all services |
| Error → HTTP mapping | built-in (`HTTPStatusFromCode`) | local copy of a small table | built-in, 4xx/5xx natively |
| OpenAPI docs | generated (openapiv2) | none | generated (OpenAPI via reflection) |
| Blast radius | gateway + protos only | gateway only | all services |
| Fits existing `buf generate` flow | yes | n/a | would extend to service codegen |

## Recommendation input

Both A and B satisfy the Phase 4 requirements; they differ on where the
REST surface lives. A buys proto-driven lockstep and generated OpenAPI at
the cost of the repo's first `googleapis` dependency and a bigger buf
pipeline; B keeps the gateway the only new surface (no proto changes) and
matches the repo's existing sqlc-over-ORM, explicit-code style. C is the
strongest long-term protocol story but requires touching every service, so
it is a roadmap decision, not a Phase 4 one. Deciding factors worth weighing
in the spec: whether generated OpenAPI matters for the product, and whether
REST route ownership should live in protos (reviewable with the schema) or
in gateway code.
