DOCKER := env("DOCKER", "docker")

# Container settings
REDIS_CONTAINER  := "lq-test-redis"
REDIS_PORT       := "6399"
REDIS_IMAGE      := "redis:7-alpine"

MONGO_CONTAINER  := "lq-test-mongo"
MONGO_PORT       := "27019"
MONGO_IMAGE      := "mongo:7.0"

PG_CONTAINER     := "lq-test-pg"
PG_PORT          := "5433"
PG_IMAGE         := "postgres:16-alpine"
PG_USER          := "littleq_test"
PG_PASS          := "littleq_test"
PG_DB            := "littleq_test"

# Default: list recipes
default:
    @just --list

# Build all packages
build:
    go build ./...

# Run unit tests (no external services)
test:
    go test ./...

test-v:
    go test -v ./...

test-race:
    go test -race ./...

test-cover:
    go test -cover ./...

# Run all integration tests (Redis + MongoDB + PostgreSQL)
test-integration: redis-start mongo-start pg-start
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'just redis-stop mongo-stop pg-stop' EXIT
    REDIS_URL="redis://localhost:{{REDIS_PORT}}" \
    MONGO_URI="mongodb://localhost:{{MONGO_PORT}}/?directConnection=true" \
    POSTGRES_DSN="postgres://{{PG_USER}}:{{PG_PASS}}@localhost:{{PG_PORT}}/{{PG_DB}}?sslmode=disable" \
    go test -v -race -count=1 -tags integration ./...

# Run Redis integration tests only
test-redis: redis-start
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'just redis-stop' EXIT
    REDIS_URL="redis://localhost:{{REDIS_PORT}}" \
    go test -v -count=1 -tags integration ./redis/...

# Run MongoDB integration tests only (replica set enabled)
test-mongo: mongo-start
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'just mongo-stop' EXIT
    MONGO_URI="mongodb://localhost:{{MONGO_PORT}}/?directConnection=true" \
    go test -v -count=1 -tags integration ./mongo/...

# Run PostgreSQL integration tests only
test-pg: pg-start
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'just pg-stop' EXIT
    POSTGRES_DSN="postgres://{{PG_USER}}:{{PG_PASS}}@localhost:{{PG_PORT}}/{{PG_DB}}?sslmode=disable" \
    go test -v -count=1 -tags integration ./postgres/...

# --- Redis ---

redis-start:
    #!/usr/bin/env bash
    set -euo pipefail
    if {{DOCKER}} ps -a --format '{{{{.Names}}}}' | grep -q "^{{REDIS_CONTAINER}}$"; then
        {{DOCKER}} rm -f {{REDIS_CONTAINER}} > /dev/null
    fi
    echo "Starting Redis on port {{REDIS_PORT}}..."
    {{DOCKER}} run -d --name {{REDIS_CONTAINER}} -p {{REDIS_PORT}}:6379 {{REDIS_IMAGE}}
    until {{DOCKER}} exec {{REDIS_CONTAINER}} redis-cli ping 2>/dev/null | grep -q PONG; do sleep 0.3; done
    echo "Redis ready"

redis-stop:
    #!/usr/bin/env bash
    if {{DOCKER}} ps -a --format '{{{{.Names}}}}' | grep -q "^{{REDIS_CONTAINER}}$"; then
        {{DOCKER}} rm -f {{REDIS_CONTAINER}} > /dev/null && echo "Redis stopped"
    fi

# --- MongoDB (replica set) ---

mongo-start:
    #!/usr/bin/env bash
    set -euo pipefail
    if {{DOCKER}} ps -a --format '{{{{.Names}}}}' | grep -q "^{{MONGO_CONTAINER}}$"; then
        {{DOCKER}} rm -f {{MONGO_CONTAINER}} > /dev/null
    fi
    echo "Starting MongoDB replica set on port {{MONGO_PORT}}..."
    {{DOCKER}} run -d --name {{MONGO_CONTAINER}} -p {{MONGO_PORT}}:27017 {{MONGO_IMAGE}} --replSet rs0
    echo "Waiting for MongoDB to start..."
    for i in $(seq 1 30); do
        if {{DOCKER}} exec {{MONGO_CONTAINER}} mongosh --quiet --eval "db.adminCommand('ping').ok" 2>/dev/null | grep -q "1"; then
            break
        fi
        sleep 1
    done
    echo "Initialising replica set..."
    {{DOCKER}} exec {{MONGO_CONTAINER}} mongosh --quiet --eval \
        "rs.initiate({_id:'rs0',members:[{_id:0,host:'localhost:27017'}]})" > /dev/null
    echo "Waiting for primary election..."
    for i in $(seq 1 30); do
        if {{DOCKER}} exec {{MONGO_CONTAINER}} mongosh --quiet --eval \
            "rs.isMaster().ismaster" 2>/dev/null | grep -q "true"; then
            break
        fi
        sleep 1
    done
    echo "MongoDB replica set ready on port {{MONGO_PORT}}"

mongo-stop:
    #!/usr/bin/env bash
    if {{DOCKER}} ps -a --format '{{{{.Names}}}}' | grep -q "^{{MONGO_CONTAINER}}$"; then
        {{DOCKER}} rm -f {{MONGO_CONTAINER}} > /dev/null && echo "MongoDB stopped"
    fi

# --- PostgreSQL ---

pg-start:
    #!/usr/bin/env bash
    set -euo pipefail
    if {{DOCKER}} ps -a --format '{{{{.Names}}}}' | grep -q "^{{PG_CONTAINER}}$"; then
        {{DOCKER}} rm -f {{PG_CONTAINER}} > /dev/null
    fi
    echo "Starting PostgreSQL on port {{PG_PORT}}..."
    {{DOCKER}} run -d --name {{PG_CONTAINER}} \
        -p {{PG_PORT}}:5432 \
        -e POSTGRES_USER={{PG_USER}} \
        -e POSTGRES_PASSWORD={{PG_PASS}} \
        -e POSTGRES_DB={{PG_DB}} \
        {{PG_IMAGE}}
    for i in $(seq 1 30); do
        if {{DOCKER}} exec {{PG_CONTAINER}} pg_isready -U {{PG_USER}} > /dev/null 2>&1; then
            echo "PostgreSQL ready on port {{PG_PORT}}"
            exit 0
        fi
        sleep 1
    done
    echo "PostgreSQL failed to start" >&2
    exit 1

pg-stop:
    #!/usr/bin/env bash
    if {{DOCKER}} ps -a --format '{{{{.Names}}}}' | grep -q "^{{PG_CONTAINER}}$"; then
        {{DOCKER}} rm -f {{PG_CONTAINER}} > /dev/null && echo "PostgreSQL stopped"
    fi

# --- Protobuf ---

# Regenerate Go code from .proto files (requires mise tools)
proto:
    protoc \
        --go_out=. \
        --go_opt=paths=source_relative \
        --go-grpc_out=. \
        --go-grpc_opt=paths=source_relative \
        proto/littleq/v1/scheduler.proto

# --- Dev / Lint ---

lint:
    golangci-lint run

fmt:
    go fmt ./...

tidy:
    go mod tidy

vulncheck:
    govulncheck ./...

security:
    gosec ./...

bench:
    go test -bench=. -benchmem ./...

tools:
    mise install

# Tag and push a new patch release
release:
    #!/usr/bin/env bash
    set -euo pipefail
    latest=$(git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0")
    IFS='.' read -r major minor patch <<< "${latest#v}"
    next="v${major}.${minor}.$((patch + 1))"
    echo "Releasing $next"
    git tag "$next"
    git push origin "$next"
