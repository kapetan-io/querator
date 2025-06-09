.DEFAULT_GOAL := build
LINT = $(GOPATH)/bin/golangci-lint
LINT_VERSION = v1.64.6
VERSION=$(shell git describe --tags --exact-match 2>/dev/null || echo "dev-build")

.PHONY: proto
proto: ## Build protos
	./buf.gen.yaml

$(LINT): ## Download Go linter
	curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOPATH)/bin $(LINT_VERSION)

.PHONY: test
test:
	go test -timeout 10m -v -p=1 -count=1 -race -logging=ci ./...

.PHONY: cover
cover:
	-rm coverage.html coverage.out
	go test -timeout 10m -v -p=1 -count=1 -logging=ci --coverprofile=coverage.out -covermode=atomic -coverpkg=./... ./...
	go tool cover -html=coverage.out -o coverage.html
	open coverage.html


.PHONY: lint
lint: $(LINT) ## Run Go linter
	$(LINT) run -v ./...

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

.PHONY: docker
docker: ## Build Docker image
	docker build --build-arg VERSION=$(VERSION) -t ghcr.io/kapetan-io/querator:$(VERSION) .
	docker tag ghcr.io/kapetan-io/querator:$(VERSION) ghcr.io/kapetan-io/querator:latest

