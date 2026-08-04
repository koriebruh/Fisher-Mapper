.PHONY: build run run-worker migrate-up migrate-down docker-up docker-down

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
