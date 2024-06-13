.DEFAULT_GOAL := build

.PHONY: proto
proto: ## Build protos
	./buf.gen.yaml