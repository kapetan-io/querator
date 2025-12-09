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
	TEST_LOGGING=ci go test -timeout 10m -v -p=1 -count=1 -race $$(go list ./... | grep -v /benchmarks)

.PHONY: cover
cover:
	-rm coverage.html coverage.out
	TEST_LOGGING=ci go test -timeout 10m -v -p=1 -count=1 --coverprofile=coverage.out -covermode=atomic -coverpkg=./... $$(go list ./... | grep -v /benchmarks)
	go tool cover -html=coverage.out -o coverage.html
	open coverage.html

.PHONY: benchmark
benchmark: ## Run all benchmarks
	go test -bench=. -benchmem -timeout 30m ./benchmarks/...

.PHONY: benchmark-produce
benchmark-produce: ## Run produce benchmarks only
	go test -bench=BenchmarkProduceOperations -benchmem -timeout 10m ./benchmarks/...

.PHONY: benchmark-throughput
benchmark-throughput: ## Run throughput benchmarks
	go test -bench=BenchmarkThroughput -benchmem -timeout 10m ./benchmarks/...


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
	go test -fuzz=FuzzQueueInvariant -fuzztime=10m ./service/...

.PHONY: docker
docker: ## Build Docker image
	docker build --build-arg VERSION=$(VERSION) -t ghcr.io/kapetan-io/querator:$(VERSION) .
	docker tag ghcr.io/kapetan-io/querator:$(VERSION) ghcr.io/kapetan-io/querator:latest

