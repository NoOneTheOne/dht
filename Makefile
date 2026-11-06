APP_SPIDER   := dht-spider
APP_SEARCH   := dht-search
APP_GETPEERS := dht-getpeers
CMD_SPIDER   := ./cmd/spider
CMD_SEARCH   := ./cmd/search
CMD_GETPEERS := ./cmd/getpeers

BUILD_DIR := build
LDFLAGS   := -s -w

GO := go

.PHONY: all build build-spider build-search build-getpeers test vet lint fmt clean docker

all: build

build: build-spider build-search build-getpeers

build-spider:
	@mkdir -p $(BUILD_DIR)
	$(GO) build -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(APP_SPIDER) $(CMD_SPIDER)

build-search:
	@mkdir -p $(BUILD_DIR)
	$(GO) build -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(APP_SEARCH) $(CMD_SEARCH)

build-getpeers:
	@mkdir -p $(BUILD_DIR)
	$(GO) build -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(APP_GETPEERS) $(CMD_GETPEERS)

test:
	$(GO) test -race -count=1 ./...

vet:
	$(GO) vet ./...

lint:
	golangci-lint run ./...

fmt:
	gofmt -s -w .

clean:
	rm -rf $(BUILD_DIR)
	$(GO) clean

docker:
	docker compose build

docker-up:
	docker compose up -d

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f
