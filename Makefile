.PHONY: help deps

help: ## Shows this help message
	@echo 'Available targets:'; \
	grep -E '^[a-zA-Z0-9_-]+:.*##' $(MAKEFILE_LIST) \
	| sed -E 's/^([a-zA-Z0-9_-]+):.*##[ ]?(.*)/  \1    \2/'; \
	echo ""

clean: ## Clean up environment
	@echo "Cleaning up environment..."
	rm -rf downloads/

deps: deps-uv deps-dagster ## Install base dependencies
	@echo "Installing base dependencies..."

deps-uv:  ## Install uv
	@echo "Installing uv..."
	curl -LsSf https://astral.sh/uv/install.sh | sh

deps-dagster: deps-uv ## Install dagster
	@echo "Installing dagster..."
	uv add dagster dagster-webserver dagster-dg-cli sqlmodel psycopg2-binary python-dotenv
	source .venv/bin/activate

db-up: ## Start PostgreSQL database using Docker
	@echo "Starting PostgreSQL database..."
	@if docker ps -q --filter "name=entomlgist-postgres" | grep -q .; then \
		echo "Database already running"; \
	elif docker ps -aq --filter "name=entomlgist-postgres" | grep -q .; then \
		echo "Starting existing container..."; \
		docker start entomlgist-postgres; \
	else \
		echo "Creating new container..."; \
		docker run -d \
			--name entomlgist-postgres \
			-e POSTGRES_USER=entomlgist \
			-e POSTGRES_PASSWORD=entomlgist_dev_password \
			-e POSTGRES_DB=entomlgist \
			-p 5432:5432 \
			-v $$(pwd)/postgres_data:/var/lib/postgresql/data \
			postgres:16-alpine; \
	fi
	@echo "Waiting for database to be ready..."
	@until docker exec entomlgist-postgres pg_isready -U entomlgist -q 2>/dev/null; do \
		echo "Waiting for database..."; \
		sleep 1; \
	done
	@echo "Database is ready!"

db-down: ## Stop PostgreSQL database
	@echo "Stopping PostgreSQL database..."
	@docker stop entomlgist-postgres 2>/dev/null || true

db-remove: ## Remove PostgreSQL container
	@echo "Removing PostgreSQL container..."
	@docker rm -f entomlgist-postgres 2>/dev/null || true

db-logs: ## View PostgreSQL database logs
	@echo "Viewing database logs..."
	@docker logs -f entomlgist-postgres

db-reset: ## Reset PostgreSQL database (WARNING: deletes all data)
	@echo "Resetting database..."
	@docker rm -f entomlgist-postgres 2>/dev/null || true
	@rm -rf postgres_data
	@echo "Database reset complete. Run 'make db-up' to start fresh."

db-shell: ## Open PostgreSQL shell
	@echo "Opening database shell..."
	@docker exec -it entomlgist-postgres psql -U entomlgist -d entomlgist

dagster-dev: db-up ## Run dagster server (starts DB if needed)
	@echo "Running dagster server..."
	dg dev

dagster-full-reddit-pipeline: db-up ## Run full reddit pipeline to download, process, and load data
	@echo "Running full reddit pipeline..."
	uv run dg launch --job full_reddit_pipeline_job
