# DSNs match docker-compose.yml. Override on the command line to point at
# clusters made by scripts/setup-local.sh, or anywhere else.
SOURCE_DSN ?= postgres://postgres:ferryman@127.0.0.1:5433/ferryman
TARGET_DSN ?= postgres://postgres:ferryman@127.0.0.1:5434/ferryman

.PHONY: build test test-unit up down migrate status clean

build:
	go build ./...

# The whole suite. Most of it needs two live postgres instances: without the DSNs
# those tests skip and `go test` still says ok, which is why this target sets them
# rather than leaving it to the shell.
test: up
	FERRYMAN_SOURCE_DSN="$(SOURCE_DSN)" FERRYMAN_TARGET_DSN="$(TARGET_DSN)" \
		go test ./... -count=1 -v

# Just the parts that need no database, for a fast loop.
test-unit:
	go test ./... -count=1 -short

up:
	docker compose up -d --wait

down:
	docker compose down -v

migrate: build
	go run ./cmd/ferryman migrate -source "$(SOURCE_DSN)" -target "$(TARGET_DSN)"

status:
	go run ./cmd/ferryman status -source "$(SOURCE_DSN)"

clean: down
	go clean ./...
