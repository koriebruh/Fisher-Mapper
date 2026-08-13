.PHONY: build run run-worker migrate-up migrate-down docker-up docker-down test test-db proto

BINARY        := bin/server
WORKER_BINARY := bin/worker
MIGRATE_BIN   := bin/migrate

# Plugin versions are pinned explicitly (not @latest) so `make proto`
# reproduces the exact stubs already committed under
# internal/transport/grpc/pb/ -- these track go.mod's
# google.golang.org/protobuf and google.golang.org/grpc versions, and must
# be bumped together with them (see proto/README notes in payment.proto's
# header comment history / PR description for the pairing: protoc-gen-go
# v1.36.11 <-> google.golang.org/protobuf v1.36.11, otelgrpc v0.69.0 <->
# google.golang.org/grpc v1.81.1).
PROTOC_GEN_GO_VERSION       := v1.36.11
PROTOC_GEN_GO_GRPC_VERSION  := v1.6.2

# proto regenerates internal/transport/grpc/pb/ from proto/payment/v1/*.proto
# via buf (github.com/bufbuild/buf -- installable with
# `go install github.com/bufbuild/buf/cmd/buf@latest`, no protoc binary
# needed: buf ships its own protobuf compiler). Generated Go stubs ARE
# committed to the repo (see internal/transport/grpc/pb/ -- not gitignored):
# not every developer building this template has buf/protoc installed, and
# gRPC clients/servers need the stubs to compile at all, so "generated but
# uncommitted" would make a fresh clone fail to build.
proto:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)
	buf generate

build:
	go build -o $(BINARY) ./cmd/server
	go build -o $(WORKER_BINARY) ./cmd/worker
	go build -o $(MIGRATE_BIN) ./cmd/migrate

run: build
	./$(BINARY)

run-worker: build
	./$(WORKER_BINARY)

migrate-up: build
	./$(MIGRATE_BIN)

migrate-down: build
	./$(MIGRATE_BIN) -down

docker-up:
	docker compose up -d

docker-down:
	docker compose down

# test runs everything that doesn't need Postgres (TEST_POSTGRES_DSN unset
# -> DB-gated tests skip themselves).
test:
	go test ./... -race

# test-db runs the full suite, including DB-gated integration tests, against
# the docker-compose Postgres (must already be up + migrated). -p 1 forces
# package test binaries to run one at a time: several packages
# (internal/domain/payment, internal/messaging/outbox, internal/messaging/webhook) hit the same
# shared tables, and plain `go test ./...` runs different packages'
# binaries as concurrent OS processes, which is otherwise a source of
# cross-package flakiness on shared DB state. Most of that flakiness is
# additionally guarded against directly in the tests themselves
# (membership-based assertions rather than exact global counts, e.g.
# internal/messaging/outbox's claim tests) -- -p 1 removes the remaining sliver.
test-db:
	TEST_POSTGRES_DSN="postgres://fisher:fisher@localhost:5432/fisher_mapper?sslmode=disable" go test ./... -race -p 1 -count=1
