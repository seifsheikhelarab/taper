# Containerizing the Go services, and whether nginx should load-balance them

**Question.** Should the five Go binaries (`stock`, `reservation`, `order`,
`fulfillment`, `gateway`) run as separate slim containers in compose — and
should there be load balancers, "maybe nginx", in front of them?

**Researched**: 2026-09-11, against primary sources only (Docker docs,
grpc.io, grpc/grpc design docs, nginx module docs, distroless README).
Repo state cross-checked against `docker-compose.yml`, `cmd/*`, and the
Phase 5 load results in `docs/runbooks/load.md`.

---

## TL;DR

1. **Containerize: yes.** Official multi-stage Dockerfiles (one per binary)
   on `gcr.io/distroless/static-debian13:nonroot` (or `scratch` + explicit
   `USER`) produce ~10–15 MiB images and are the documented Docker/Golang
   pattern. Low risk, no architectural change.
2. **Load balancers between our own services: no — not with a proxy, and
   not by default even client-side.** gRPC load balancing is *per-call*
   multiplexed over long-lived HTTP/2 connections. An L4 proxy like nginx's
   TCP stream module sees **one connection** and pins it to one upstream —
   it does not balance calls. And grpc-go's default LB policy is
   `pick_first`, which keeps all RPCs on the first reachable replica; to
   spread across replicas you must opt into `round_robin`.
3. **nginx specifically: only as the front door for the HTTP gateway, and
   only when needed** (TLS termination, HTTP/1 external clients, ingress
   routing). It is the wrong tool between internal gRPC hops. Even there it
   has a sharp edge: `grpc_next_upstream` defaults to `error timeout` and
   POSTs are **not** retried unless `non_idempotent` is set — for a
   reservation API that is exactly the behavior you want (never silently
   retry a reserve), but it must be a conscious choice.

---

## Part 1 — Slim containers per service

### What the sources say

- Docker's official [Go language guide](https://docs.docker.com/guides/golang/)
  prescribes multi-stage builds: a `golang` builder stage running
  `CGO_ENABLED=0 GOOS=linux go build`, then a minimal runtime stage. The
  guide's worked example ships exactly this Dockerfile pattern.
- Google's [distroless README](https://github.com/GoogleContainerTools/distroless)
  recommends restricting the runtime image to "precisely what's necessary":
  `static-debian13` is ~2 MiB (vs alpine ~5 MiB, debian 124 MiB), contains
  no shell or package manager, and requires `ENTRYPOINT` in vector form.
  `CGO_ENABLED=0` binaries land on the `static` variant.

### Our binaries qualify

Every `cmd/*` binary is a pure-Go static binary today: pgx (pure Go),
kafka-go, grpc-go, otel — no cgo anywhere in `go.mod`. `CGO_ENABLED=0`
is safe, so the `static` (nonroot) variant is the right base.

### Recommended shape (one Dockerfile, build-arg the binary)

```dockerfile
# syntax=docker/dockerfile:1
ARG GO_VERSION=1.25
FROM golang:${GO_VERSION} AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG BIN
RUN CGO_ENABLED=0 go build -trimpath -o /out/app ./cmd/${BIN}

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/app /app
ENTRYPOINT ["/app"]
```

- `docker compose build --build-arg BIN=stock` (or five thin Dockerfiles
  extending one base) keeps it to a single pattern.
- Today compose is **deliberately infra-only** (recorded on PR #43 and
  #51): Go services run locally, which keeps the inner dev loop fast.
  Adding service containers should be an opt-in compose **profile**
  (`--profile containers`) so neither workflow is sacrificed.
- Caveat worth recording: the Phase 5 findings that motivated
  `database.OpenPool` and the fsync analysis were measured with binaries
  **outside** Docker. Containerizing adds no new queueing by itself, but
  re-run the k6 harness with containers before comparing numbers.

---

## Part 2 — Load balancing gRPC: what the primary docs actually say

### gRPC load balancing is per-call, over long-lived connections

grpc/grpc's [load-balancing design doc](https://github.com/grpc/grpc/blob/master/doc/load-balancing.md):

> "Load-balancing within gRPC happens on a per-call basis, not a
> per-connection basis. In other words, even if all requests come from a
> single client, we still want them to be load-balanced across all servers."

and the grpc.io [load balancing overview](https://grpc.io/blog/grpc-load-balancing/):

> "In transport level [L4] load balancing, the server terminates the TCP
> connection and opens another connection to the backend of choice."

**Consequence for us:** HTTP/2 multiplexes many RPCs over one TCP
connection. An L4 balancer (nginx `stream`, cloud NLBs, Docker's port
publishing, kube-proxy) forwards that single connection to a single
upstream for its whole lifetime — so per-call balance silently degrades to
per-*process* balance. grpc.io's own recommendation table says use L4 only
when "minimizing resource utilization in proxy is more important than
features" / "latency is paramount", and L7 (connection-aware) for "RPC load
varies a lot among connections".

### The default policy would defeat scaling anyway

From the same design doc, grpc-go's **default LB policy is `pick_first`**:
it connects to the first reachable address and "all RPCs sent on the
channel will be sent to that address". A second replica would sit idle.
Options that actually distribute:

- **Client-side `round_robin`**: configure the service config / channel
  `grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"round_robin":{}}]}`)`
  plus a resolver that returns all replicas (headless-style DNS: grpc-go's
  resolver re-resolves and picks up new addresses). Zero new moving parts;
  each client keeps a subchannel per replica.
- **Lookaside LB / xDS**: the modern control-plane path, but far beyond
  this project's needs.
- grpc.io's table for "microservices — N clients, M servers" with trusted
  clients and high-performance requirements points at **client-side LB**
  — which is exactly our topology (our clients are our own services).

### nginx between gRPC hops: wrong tool here

nginx's [`ngx_http_grpc_module`](https://nginx.org/en/docs/http/ngx_http_grpc_module.html)
does L7 gRPC proxying with upstream groups and round-robin. It works, but:

- It sits **in the data path of every internal call**, adding a hop the
  grpc.io table lists as the main proxy con ("LB is in the data path,
  higher latency, LB throughput may limit scalability").
- It breaks the Fail-Fast design: our gateway already implements
  per-dependency circuit breakers that open to 503 on downstream failure.
  A nginx `upstream` group doing its own retry/masking would hide
  downstream health from the breakers and blur the RED metrics per replica.
- `grpc_next_upstream` default is `error timeout` and, per the docs,
  "requests with a non-idempotent method (POST, LOCK, PATCH) are not passed
  to the next server" unless `non_idempotent` is enabled. For
  `ReserveStock` (POST) that's *correct* — a retry could double-reserve
  before Idempotency-Key handling — but it means a naive nginx setup
  silently gives no failover on our most important verb.

### Where a proxy *does* belong

- **In front of `cmd/gateway`** (REST/HTTP1.1 external surface): TLS
  termination, HTTP/2 for external gRPC-Web-style clients later, and a
  stable public endpoint. nginx here is conventional and cheap. The gateway
  remains the single gRPC client of the internal services — where
  client-side `round_robin` handles replica fan-out with no extra hop.
- If/when we outgrow hand-rolled placement (autoscaling, many services),
  the honest answer is a service mesh / xDS data plane, not a chain of
  nginx hops.

---

## Recommended sequence

1. **Do now (cheap, no risk):** multi-stage distroless Dockerfiles behind
   an opt-in compose profile; pin `DB_POOL_MAX_CONNS` per deployment.
   **[Implemented 2026-09-11](https://github.com/seifsheikhelarab/taper/commit/4610ccc):**
   one multi-stage Dockerfile (single cached compile pass) +
   `profiles: ["containers"]` compose services, saga smoke-tested.
2. **When scaling matters:** DNS-based replica discovery +
   `round_robin` client-side LB in the gateway's three dials (and
   fulfillment's stock dial), replicas behind a headless-style service
   name, no proxy in the internal path.
   **[Implemented 2026-09-11](https://github.com/seifsheikhelarab/taper/commit/4610ccc):**
   `pkg/grpcx.Dial` sets `round_robin` and is used by all four client
   dials; live-verified with `--scale stock=2` — 20 calls split exactly
   10/10 across replicas (per-replica RED counters). Two operational
   caveats: compose `--scale` is not persisted across subsequent `up`s,
   and scale-*up* discovery waits for the resolver's next periodic
   re-resolve (~30 min for grpc-go's dns resolver) while scale-*down* is
   picked up immediately on subchannel failure.
3. **At the perimeter, when needed:** nginx (or any L7 ingress) in front of
   the gateway for TLS + external routing; keep POST retry semantics off.

## Sources

- Docker, "Go language-specific guide" — multi-stage pattern:
  https://docs.docker.com/guides/golang/
- GoogleContainerTools/distroless README (image sizes, `static-debian13`,
  vector ENTRYPOINT): https://github.com/GoogleContainerTools/distroless
- grpc/grpc, "Load Balancing in gRPC" (per-call semantics, `pick_first`
  default, `round_robin`, `grpclb` deprecation):
  https://github.com/grpc/grpc/blob/master/doc/load-balancing.md
- grpc.io blog, "gRPC Load Balancing" (proxy vs client-side tradeoffs,
  L4 vs L7 guidance, recommendation table):
  https://grpc.io/blog/grpc-load-balancing/
- nginx, `ngx_http_grpc_module` docs (upstream round-robin,
  `grpc_next_upstream` defaults and the `non_idempotent` POST rule):
  https://nginx.org/en/docs/http/ngx_http_grpc_module.html
