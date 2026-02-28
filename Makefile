.PHONY: setup install run test test-quick clean help up db-up db-down migrate check-pgvector analyze
.PHONY: demo quickstart audit-setup audit-run audit-verify airflow-up airflow-down

# Internal helper target(s)
.PHONY: require-docker require-snowflake

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

require-snowflake:
	@python -c "from dotenv import load_dotenv; load_dotenv(); import os; assert os.environ.get('SNOWFLAKE_ACCOUNT'), 'Set SNOWFLAKE_ACCOUNT in .env'" 2>/dev/null || ( \
		echo "Error: SNOWFLAKE_ACCOUNT not set in .env"; \
		echo ""; \
		echo "Add these to your .env file:"; \
		echo "  SNOWFLAKE_ACCOUNT=your_org-your_account"; \
		echo "  SNOWFLAKE_USER=your_user"; \
		echo "  SNOWFLAKE_PASSWORD=your_password"; \
		exit 1; \
	)

# Default target
help:
	@echo "Air Logistics Control Plane - Make Commands"
	@echo ""
	@echo "Quick Start:"
	@echo "  make quickstart   - Full setup + demo (fill .env first)"
	@echo ""
	@echo "Setup:"
	@echo "  make setup        - Full setup (venv, deps, migrations, verify)"
	@echo "  make install      - Install dependencies only"
	@echo ""
	@echo "Run:"
	@echo "  make run          - Start the API server"
	@echo "  make demo         - Run 3 scenarios against running server"
	@echo ""
	@echo "Test:"
	@echo "  make test         - Run all tests (requires DB)"
	@echo "  make test-quick   - Run tests without database"
	@echo ""
	@echo "Audit Warehouse (Snowflake):"
	@echo "  make audit-setup  - Create Snowflake schema + Cortex Search services"
	@echo "  make audit-run    - Extract from Postgres → load to Snowflake → verify"
	@echo "  make audit-verify - Run data quality checks on Snowflake"
	@echo ""
	@echo "Airflow (requires Docker):"
	@echo "  make airflow-up   - Start Airflow (webserver + scheduler)"
	@echo "  make airflow-down - Stop Airflow"
	@echo ""
	@echo "Database (Docker):"
	@echo "  make up           - Start Postgres via Docker"
	@echo "  make down         - Stop Postgres"
	@echo "  make migrate      - Run database migrations"
	@echo ""
	@echo "Clean:"
	@echo "  make clean        - Remove venv and cache"

# ============================================================
# Quick Start: one command after filling .env
# ============================================================

quickstart:
	@echo "=========================================="
	@echo "  Quick Start: setup + demo"
	@echo "=========================================="
	@./setup.sh --no-prompt
	@echo ""
	@echo "Starting server in background..."
	@python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 &
	@echo "Waiting for server to be ready..."
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
		curl -s http://localhost:8000/docs > /dev/null 2>&1 && break; \
		sleep 1; \
	done
	@echo ""
	@$(MAKE) demo
	@echo ""
	@echo "=========================================="
	@echo "  Server running at http://localhost:8000"
	@echo "  UI at http://localhost:8000/ui/"
	@echo "  API docs at http://localhost:8000/docs"
	@echo "=========================================="
	@echo ""
	@echo "To also set up the Snowflake audit warehouse:"
	@echo "  make audit-setup    # create schema + Cortex Search"
	@echo "  make audit-run      # extract + load + verify"

# ============================================================
# Setup
# ============================================================

setup:
	@./setup.sh

install:
	pip install -e ".[dev,llm]"

# ============================================================
# Run
# ============================================================

run:
	python -m uvicorn app.main:app --reload --port 8000

# Run 3 demo scenarios against the running server
demo:
	@echo "Running demo scenarios..."
	@echo ""
	@echo "[1/3] JFK ground stop..."
	@curl -s -X POST http://localhost:8000/simulation/run/jfk_ground_stop | python -m json.tool 2>/dev/null | head -5 || echo "  Failed (is the server running?)"
	@echo ""
	@echo "[2/3] ORD thunderstorm..."
	@curl -s -X POST http://localhost:8000/simulation/run/ord_thunderstorm | python -m json.tool 2>/dev/null | head -5 || echo "  Failed"
	@echo ""
	@echo "[3/3] LAX clear skies..."
	@curl -s -X POST http://localhost:8000/simulation/run/lax_clear_skies | python -m json.tool 2>/dev/null | head -5 || echo "  Failed"
	@echo ""
	@echo "Demo complete. View packets at http://localhost:8000/packets"
	@echo "Or open the UI at http://localhost:8000/ui/"

# ============================================================
# Test
# ============================================================

test:
	pytest tests/ -v

test-quick:
	pytest tests/ -v -m "not requires_db"

# ============================================================
# Audit Warehouse (Snowflake)
# ============================================================

# Create Snowflake schema + both Cortex Search services
audit-setup:
	@$(MAKE) require-snowflake
	@echo "Setting up Snowflake audit warehouse..."
	python -m audit_warehouse.run_sql --all
	@echo ""
	@echo "Snowflake setup complete."
	@echo "Next: make audit-run (extract + load + verify)"

# Full pipeline: extract from Postgres → load to Snowflake → verify
audit-run:
	@$(MAKE) require-snowflake
	@echo "Running audit pipeline..."
	python -m audit_warehouse.load preflight
	python -m audit_warehouse.load run
	python -m audit_warehouse.load verify
	@echo ""
	@echo "Audit pipeline complete. Test RAG:"
	@echo '  curl -X POST localhost:8000/rag/query -H "Content-Type: application/json" -d '"'"'{"question":"Which airports had HOLD posture?"}'"'"''

# Just run verification checks
audit-verify:
	@$(MAKE) require-snowflake
	python -m audit_warehouse.load verify

# ============================================================
# Airflow
# ============================================================

airflow-up:
	@$(MAKE) require-docker
	@if [ ! -f airflow/.env ]; then \
		cp airflow/.env.example airflow/.env; \
		FERNET=$$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" 2>/dev/null); \
		if [ -n "$$FERNET" ]; then \
			sed -i '' "s/^AIRFLOW_FERNET_KEY=$$/AIRFLOW_FERNET_KEY=$$FERNET/" airflow/.env 2>/dev/null || true; \
		fi; \
		echo "Created airflow/.env with generated Fernet key."; \
		echo "Edit airflow/.env to add DATABASE_URL and SNOWFLAKE_* credentials."; \
	fi
	cd airflow && docker compose up -d
	@echo ""
	@echo "Airflow starting..."
	@echo "  UI: http://localhost:8080 (admin / admin)"
	@echo "  Trigger DAG: docker compose -f airflow/docker-compose.yml exec airflow-scheduler airflow dags trigger aalcp_audit_pipeline"

airflow-down:
	@$(MAKE) require-docker
	cd airflow && docker compose down

# ============================================================
# Database (Docker — alternative to Supabase)
# ============================================================

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

check-pgvector:
	@$(MAKE) require-docker
	@docker exec aalcp_postgres psql -U forwarder -d postgres \
		-c "SELECT extversion FROM pg_extension WHERE extname = 'vector';"

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
	psql "$(DATABASE_URL)" -f app/db/migrations/007_playbook_aging.sql
	@echo "Migrations complete!"

# Seed policies only (useful for re-seeding without full migrate)
seed-policies:
	@echo "Seeding governance policies..."
	psql "$(DATABASE_URL)" -f app/db/migrations/005_seed_policies.sql
	@echo "Policies seeded!"

# ============================================================
# Clean
# ============================================================

clean:
	rm -rf .venv
	rm -rf __pycache__
	rm -rf .pytest_cache
	rm -rf *.egg-info
	rm -rf var/evidence/*
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
