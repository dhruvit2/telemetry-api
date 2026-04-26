GOOS ?= linux
GOARCH ?= amd64
VERSION ?= 1.0.0
IMAGE_NAME ?= telemetry-api
IMAGE_TAG ?= latest

.PHONY: build
build:
	go build -v -o bin/telemetry-api ./cmd/server/

.PHONY: build-all
build-all:
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build -ldflags="-X main.Version=$(VERSION)" -o bin/telemetry-api ./cmd/server/

.PHONY: test
test:
	go test -v -race -coverprofile=coverage.out ./...

.PHONY: coverage
coverage: test
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

.PHONY: lint
lint:
	golangci-lint run ./...

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: vet
vet:
	go vet ./...

.PHONY: docker-build
docker-build:
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .

.PHONY: docker-run
docker-run: docker-build
	docker run -d \
		--name telemetry-api \
		--network=tsdb-network \
		-p 8082:8082 \
		-e TSDB_URL=http://influxdb-tsdb:8086 \
		-e TSDB_TOKEN=7h2UjNBHN7ApaRrwz49uyRi6sySH-NaICaNLz4ZP5ROt2Jf8lDfJqtyU_e-45STGcnvD71x5sa9dRlgb9H2kKg== \
		-e TSDB_ORG=telemetry \
		-e TSDB_BUCKET=gpu_metrics_raw \
		$(IMAGE_NAME):$(IMAGE_TAG)

.PHONY: docker-stop
docker-stop:
	docker stop telemetry-api || true
	docker rm telemetry-api || true

.PHONY: run
run:
	go run ./cmd/server/

.PHONY: run-debug
run-debug:
	LOG_LEVEL=debug PORT=8080 go run ./cmd/server/

.PHONY: clean
clean:
	rm -f bin/telemetry-api coverage.out coverage.html
	go clean

.PHONY: deps
deps:
	go mod download
	go mod tidy

.PHONY: help
help:
	@echo "Available targets:"
	@echo "  build          - Build the binary"
	@echo "  build-all      - Build for all OS/ARCH"
	@echo "  test           - Run tests"
	@echo "  coverage       - Generate coverage report"
	@echo "  lint           - Run linter"
	@echo "  fmt            - Format code"
	@echo "  vet            - Run go vet"
	@echo "  docker-build   - Build Docker image"
	@echo "  docker-run     - Run Docker container"
	@echo "  run            - Run the service"
	@echo "  run-debug      - Run with debug logging"
	@echo "  clean          - Clean build artifacts"
	@echo "  deps           - Download dependencies"
