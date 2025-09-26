.PHONY: help deps

help: ## Shows this help message
	@echo 'Available targets:'; \
	grep -E '^[a-zA-Z0-9_-]+:.*##' $(MAKEFILE_LIST) \
	| sed -E 's/^([a-zA-Z0-9_-]+):.*##[ ]?(.*)/  \1    \2/'; \
	echo ""

deps: ## Install base dependencies
	@echo "Installing base dependencies..."
	@echo "Installing uv..."
	curl -LsSf https://astral.sh/uv/install.sh | sh
