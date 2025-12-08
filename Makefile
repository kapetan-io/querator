.DEFAULT_GOAL := build
VERSION=$(shell git describe --tags --exact-match 2>/dev/null || echo "dev-build")

.PHONY: install
install: ## Install querator binary with version
	go install -ldflags "-s -w -X github.com/kapetan-io/querator.Version=$(VERSION)" ./cmd/querator

.PHONY: proto
proto: ## Build protos
	./buf.gen.yaml

.PHONY: test
test:
	go test -timeout 10m -v -p=1 -count=1 -race -logging=ci ./...

.PHONY: cover
cover:
	-rm coverage.html coverage.out
	go test -timeout 10m -v -p=1 -count=1 -logging=ci --coverprofile=coverage.out -covermode=atomic -coverpkg=./... ./...
	go tool cover -html=coverage.out -o coverage.html
	open coverage.html

.PHONY: benchmark
benchmark:
	@echo "Generating structured benchmark data..."
	@mkdir -p benchmarks/benchmark_results
	go test -v ./benchmarks -run TestRunAllBenchmarks -timeout 30m
	@echo "Benchmark data saved to benchmarks/benchmark_results/"

.PHONY: benchmark-throughput
benchmark-throughput:
	@echo "Running throughput benchmark (30s per config, ~2.5 minutes)..."
	@mkdir -p benchmarks/benchmark_results
	go test -v ./benchmarks -run TestThroughputBenchmark -timeout 10m
	@echo "Throughput results saved to benchmarks/benchmark_results/throughput_results.csv"


.PHONY: lint
lint: ## Run Go linter
	golangci-lint run -v ./...

.PHONY: tidy
tidy:
	go mod tidy && git diff --exit-code

.PHONY: ci
ci: tidy lint test
	@echo
	@echo "\033[32mEVERYTHING PASSED!\033[0m"

.PHONY: vet
vet:
	go vet ./...

.PHONY: fuzz
fuzz: ## Run fuzz tests for 10 minutes
	go test -fuzz=FuzzQueueInvariant -fuzztime=10m .

.PHONY: docker
docker: ## Build Docker image
	docker build --build-arg VERSION=$(VERSION) -t ghcr.io/kapetan-io/querator:$(VERSION) .
	docker tag ghcr.io/kapetan-io/querator:$(VERSION) ghcr.io/kapetan-io/querator:latest

