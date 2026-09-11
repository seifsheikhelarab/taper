# syntax=docker/dockerfile:1
# One Dockerfile for all five service binaries (docs/research/0002):
#   docker compose --profile containers build <service>
# The build stage compiles ALL binaries once (shared across the five image
# builds, cached by BuildKit); each runtime stage copies just its binary.
# All binaries are pure Go (CGO_ENABLED=0), so the runtime is
# distroless/static: no shell, no package manager, ~2 MiB base + binary.
ARG GO_VERSION=1.26

FROM golang:${GO_VERSION} AS build
WORKDIR /src
# Cache module downloads independently of source changes.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
# One compile pass for all five binaries; cache mounts keep rebuilds
# incremental instead of recompiling the world per image.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath \
      -o /out/ \
      ./cmd/stock ./cmd/reservation ./cmd/order ./cmd/fulfillment ./cmd/gateway

FROM gcr.io/distroless/static-debian13:nonroot
ARG BIN
COPY --from=build /out/${BIN} /app
# Vector form: distroless has no shell to prefix.
ENTRYPOINT ["/app"]
