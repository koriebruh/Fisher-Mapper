.PHONY: build run run-worker migrate-up migrate-down docker-up docker-down test test-db

BINARY        := bin/server
WORKER_BINARY := bin/worker
MIGRATE_BIN   := bin/migrate

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
# (internal/domain/payment, internal/outbox, internal/webhook) hit the same
# shared tables, and plain `go test ./...` runs different packages'
# binaries as concurrent OS processes, which is otherwise a source of
# cross-package flakiness on shared DB state. Most of that flakiness is
# additionally guarded against directly in the tests themselves
# (membership-based assertions rather than exact global counts, e.g.
# internal/outbox's claim tests) -- -p 1 removes the remaining sliver.
test-db:
	TEST_POSTGRES_DSN="postgres://fisher:fisher@localhost:5432/fisher_mapper?sslmode=disable" go test ./... -race -p 1 -count=1
