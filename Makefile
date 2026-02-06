.PHONY: setup install run test test-quick clean help up db-up db-down migrate check-pgvector analyze

# Internal helper target(s)
.PHONY: require-docker

require-docker:
	@command -v docker >/dev/null 2>&1 || ( \
		echo "Error: \`docker\` not found in PATH."; \
		echo ""; \
		echo "Install Docker Desktop (macOS) and ensure the CLI is available:"; \
		echo "  - https://www.docker.com/products/docker-desktop/"; \
		echo "  - After install, run: docker --version"; \
		echo ""; \
		echo "If Docker is installed but not found, restart your terminal or add it to PATH."; \
		exit 127; \
	)

# Default target
help:
	@echo "Agentic Air Logistics Control Plane - Make Commands"
	@echo ""
	@echo "Setup:"
	@echo "  make setup      - Full setup (venv, deps, env file)"
	@echo "  make install    - Install dependencies only"
	@echo ""
	@echo "Run:"
	@echo "  make run        - Start the API server"
	@echo ""
	@echo "Test:"
	@echo "  make test       - Run all tests (requires DB)"
	@echo "  make test-quick - Run tests without database"
	@echo ""
	@echo "Database (Docker):"
	@echo "  make up         - Start Postgres via Docker"
	@echo "  make down       - Stop Postgres"
	@echo "  make migrate    - Run database migrations"
	@echo "  make check-pgvector - Verify pgvector version"
	@echo "  make analyze    - Run ANALYZE for query optimization"
	@echo ""
	@echo "Clean:"
	@echo "  make clean      - Remove venv and cache"

# Setup everything
setup:
	@./setup.sh

# Install dependencies
install:
	pip install -e ".[dev,llm]"

# Run the server
run:
	python -m uvicorn app.main:app --reload --port 8000

# Run all tests
test:
	pytest tests/ -v

# Run tests that don't require database
test-quick:
	pytest tests/test_security.py tests/test_agent_non_workflow.py -v -m "not requires_db"

# Docker database commands
up:
	@$(MAKE) require-docker
	docker compose up -d
	@echo "Waiting for Postgres..."
	@sleep 3
	@docker exec aalcp_postgres pg_isready -U forwarder -d postgres || echo "Still starting..."

db-up: up

db-down:
	@$(MAKE) require-docker
	docker compose down

down: db-down

# Verify pgvector version
check-pgvector:
	@$(MAKE) require-docker
	@docker exec aalcp_postgres psql -U forwarder -d postgres \
		-c "SELECT extversion FROM pg_extension WHERE extname = 'vector';"

# Analyze for query optimization (required after bulk inserts for ivfflat)
analyze:
	@$(MAKE) require-docker
	@docker exec aalcp_postgres psql -U forwarder -d postgres \
		-c "ANALYZE embedding_case;"

# Run migrations (works with any Postgres)
migrate:
	@echo "Running migrations..."
	psql "$(DATABASE_URL)" -f app/db/migrations/001_enable_extensions.sql
	psql "$(DATABASE_URL)" -f app/db/migrations/002_core_schema.sql
	psql "$(DATABASE_URL)" -f app/db/migrations/003_constraints_triggers.sql
	psql "$(DATABASE_URL)" -f app/db/migrations/004_indexes.sql
	psql "$(DATABASE_URL)" -f app/db/migrations/005_seed_policies.sql
	psql "$(DATABASE_URL)" -f app/db/migrations/006_evidence_dedup.sql
	@echo "Migrations complete!"

# Seed policies only (useful for re-seeding without full migrate)
seed-policies:
	@echo "Seeding governance policies..."
	psql "$(DATABASE_URL)" -f app/db/migrations/005_seed_policies.sql
	@echo "Policies seeded!"

# Clean
clean:
	rm -rf .venv
	rm -rf __pycache__
	rm -rf .pytest_cache
	rm -rf *.egg-info
	rm -rf var/evidence/*
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
