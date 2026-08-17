# syntax=docker/dockerfile:1
#
# One Dockerfile, two runtime targets (`server` and `worker`), built via
# `docker build --target server` / `--target worker`. Both binaries share a
# single build stage/cache; the final stages differ only in which compiled
# binary and which ports they expose -- matching the "two separate
# processes, one wiring source of truth" split cmd/server/main.go and
# cmd/worker/main.go's own doc comments describe (server is HTTP/gRPC-only
# and never touches Redis on the request path; worker is the only process
# that ever calls a provider).

# ---- build stage ----
# golang:1.26.6-alpine pins the EXACT patch version go.mod's `go 1.26.6`
# directive requires -- not a floating `golang:1.26-alpine` or
# `golang:latest` -- same reproducibility discipline as every pinned tool in
# .github/workflows/ci.yml (golangci-lint, gosec, buf, protoc-gen-go).
FROM golang:1.26.6-alpine AS builder
WORKDIR /src

# Dependency layer cached separately from source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=0: every dependency this repo actually builds with (pgx/v5,
# redis/go-redis, fiber, grpc-go) is pure Go, so a fully static binary is
# possible -- that's what lets the runtime stages below be distroless/static
# instead of needing glibc/musl for cgo's sake. -trimpath/-s/-w: no
# build-machine filesystem paths or debug symbols baked into a binary that
# ships in a production image.
RUN CGO_ENABLED=0 GOFLAGS=-mod=readonly go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server
RUN CGO_ENABLED=0 GOFLAGS=-mod=readonly go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker

# ---- server runtime ----
# gcr.io/distroless/static-debian12, pinned by digest (not the floating
# "nonroot" tag) for the same reproducibility reason every other tool/base
# image in this pipeline is pinned -- digest resolved from that tag at the
# time this Dockerfile was written. Chosen over alpine: this process makes
# outbound HTTPS calls (webhook/callback delivery today, real PJP
# integrations later), and distroless/static already ships the CA bundle
# outbound TLS needs plus a built-in unprivileged "nonroot" user (uid/gid
# 65532) -- both requirements below are met without an alpine
# `--no-cache add ca-certificates` + `adduser` step. Trade-off accepted: no
# shell in the image means no in-image HEALTHCHECK; liveness is verified
# from outside the container instead (see the CI `docker` job and this
# Dockerfile's own validation notes, which curl /healthz from the host).
FROM gcr.io/distroless/static-debian12@sha256:1b7b9f0f0e0a1d2155f531db587cc48ec26aaf97ab64364225f5bf18a054e66a AS server
WORKDIR /app
COPY --from=builder /out/server /app/server
# config.toml is baked in so the image is runnable standalone (e.g. against
# docker-compose's own Postgres/Redis) with no required mount step -- nothing
# in it is a secret (see its own header comment: bootstrap-only values,
# feature flags live in Postgres app_config instead). A real deployment
# should still override what matters per-environment (DSNs, ports) via
# APP_* env vars -- see internal/platform/config/bootstrap.go's Env*
# constants -- or point APP_CONFIG_FILE at a different bind-mounted file.
COPY config.toml /app/config.toml
EXPOSE 8080 9090
USER nonroot:nonroot
ENTRYPOINT ["/app/server"]

# ---- worker runtime ----
FROM gcr.io/distroless/static-debian12@sha256:1b7b9f0f0e0a1d2155f531db587cc48ec26aaf97ab64364225f5bf18a054e66a AS worker
WORKDIR /app
COPY --from=builder /out/worker /app/worker
COPY config.toml /app/config.toml
EXPOSE 9101
USER nonroot:nonroot
ENTRYPOINT ["/app/worker"]
