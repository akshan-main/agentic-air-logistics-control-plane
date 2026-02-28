#!/bin/bash
# Setup script for Agentic Air Logistics Control Plane
#
# USAGE:
#   1. Copy .env.example to .env
#   2. Paste your credentials in .env (DATABASE_URL, and optionally OPENAI_API_KEY / ANTHROPIC_API_KEY)
#   3. Run: chmod +x setup.sh && ./setup.sh
#
# That's it! The script handles everything else.

set -e

# Support --no-prompt for non-interactive use (e.g. make quickstart)
NO_PROMPT=false
if [ "$1" = "--no-prompt" ]; then
    NO_PROMPT=true
fi

echo "========================================"
echo "  Air Logistics Control Plane - Setup"
echo "========================================"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_step() {
    echo -e "\n${BLUE}[$1/$TOTAL_STEPS]${NC} $2"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

TOTAL_STEPS=8

# ============================================================
# Step 1: Check Python version
# ============================================================
print_step 1 "Checking Python..."

# Find Python 3.11+
PYTHON_CMD=""

# Check common Python 3.11+ locations
for cmd in python3.13 python3.12 python3.11 python3; do
    if command -v $cmd &> /dev/null; then
        VERSION=$($cmd -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")' 2>/dev/null)
        MAJOR=$($cmd -c 'import sys; print(sys.version_info.major)' 2>/dev/null)
        MINOR=$($cmd -c 'import sys; print(sys.version_info.minor)' 2>/dev/null)
        if [ "$MAJOR" = "3" ] && [ "$MINOR" -ge 11 ]; then
            PYTHON_CMD=$cmd
            PYTHON_VERSION=$VERSION
            break
        fi
    fi
done

# Also check Homebrew paths on macOS
if [ -z "$PYTHON_CMD" ]; then
    for path in /opt/homebrew/bin/python3.11 /opt/homebrew/bin/python3.12 /opt/homebrew/bin/python3.13 \
                /usr/local/bin/python3.11 /usr/local/bin/python3.12 /usr/local/bin/python3.13; do
        if [ -x "$path" ]; then
            VERSION=$($path -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")' 2>/dev/null)
            MAJOR=$($path -c 'import sys; print(sys.version_info.major)' 2>/dev/null)
            MINOR=$($path -c 'import sys; print(sys.version_info.minor)' 2>/dev/null)
            if [ "$MAJOR" = "3" ] && [ "$MINOR" -ge 11 ]; then
                PYTHON_CMD=$path
                PYTHON_VERSION=$VERSION
                break
            fi
        fi
    done
fi

if [ -n "$PYTHON_CMD" ]; then
    print_success "Found Python $PYTHON_VERSION ($PYTHON_CMD)"
else
    print_error "Python 3.11+ required but not found"
    echo ""
    echo "Install Python 3.11+ using one of these methods:"
    echo ""
    echo "  Homebrew (macOS):"
    echo "    ${YELLOW}brew install python@3.11${NC}"
    echo ""
    echo "  Conda:"
    echo "    ${YELLOW}conda create -n aalcp python=3.11${NC}"
    echo "    ${YELLOW}conda activate aalcp${NC}"
    echo ""
    echo "  pyenv:"
    echo "    ${YELLOW}pyenv install 3.11.0 && pyenv local 3.11.0${NC}"
    echo ""
    exit 1
fi

# ============================================================
# Step 2: Create virtual environment
# ============================================================
print_step 2 "Setting up virtual environment..."

if [ ! -d ".venv" ]; then
    $PYTHON_CMD -m venv .venv
    print_success "Created .venv with Python $PYTHON_VERSION"
else
    # Check if existing venv has correct Python version
    VENV_VERSION=$(.venv/bin/python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")' 2>/dev/null)
    VENV_MINOR=$(.venv/bin/python -c 'import sys; print(sys.version_info.minor)' 2>/dev/null)
    if [ "$VENV_MINOR" -lt 11 ]; then
        print_warning "Existing .venv has Python $VENV_VERSION, recreating with $PYTHON_VERSION"
        rm -rf .venv
        $PYTHON_CMD -m venv .venv
        print_success "Recreated .venv with Python $PYTHON_VERSION"
    else
        print_success ".venv already exists (Python $VENV_VERSION)"
    fi
fi

# Activate venv
source .venv/bin/activate

# ============================================================
# Step 3: Install dependencies
# ============================================================
print_step 3 "Installing dependencies..."

pip install --upgrade pip -q
pip install -e ".[dev,llm]" -q 2>&1 | tail -1
print_success "Dependencies installed"

# ============================================================
# Step 4: Check .env file
# ============================================================
print_step 4 "Checking environment configuration..."

if [ ! -f ".env" ]; then
    if [ -f ".env.example" ]; then
        cp .env.example .env
        print_warning "Created .env from .env.example"
        print_error "Please edit .env with your credentials and run setup.sh again"
	        echo ""
	        echo "Required credentials in .env:"
	        echo "  - DATABASE_URL (PostgreSQL connection string)"
	        echo "  - OPENAI_API_KEY or ANTHROPIC_API_KEY (for LLM agent runs)"
	        echo ""
	        exit 1
	    else
        print_error ".env file not found and no .env.example available"
        exit 1
    fi
fi

# Check for required variables (parse via python to handle special chars in URLs)
eval "$(python3 -c "
from dotenv import dotenv_values
vals = dotenv_values('.env')
for k, v in vals.items():
    if v is not None:
        # Shell-safe export: single-quote the value
        safe = v.replace(\"'\", \"'\\\"'\\\"'\")
        print(f\"export {k}='{safe}'\")
" 2>/dev/null)" || source .env 2>/dev/null || true

if [ -z "$DATABASE_URL" ]; then
    print_error "DATABASE_URL not set in .env"
    echo "Please add your PostgreSQL connection string to .env"
    exit 1
fi

print_success ".env configured with DATABASE_URL"

# ============================================================
# Step 5: Test database connection
# ============================================================
print_step 5 "Testing database connection..."

DB_TEST=$(python3 << 'EOF'
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(".env"))

try:
    import psycopg2
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    cur = conn.cursor()
    cur.execute("SELECT version()")
    version = cur.fetchone()[0]
    cur.close()
    conn.close()
    # Extract just PostgreSQL version
    pg_version = version.split()[1] if "PostgreSQL" in version else version[:50]
    print(f"OK:{pg_version}")
except Exception as e:
    print(f"ERROR:{str(e)[:100]}")
    sys.exit(1)
EOF
)

if [[ $DB_TEST == OK:* ]]; then
    PG_VERSION="${DB_TEST#OK:}"
    print_success "Connected to PostgreSQL $PG_VERSION"
else
    ERROR_MSG="${DB_TEST#ERROR:}"
    print_error "Database connection failed: $ERROR_MSG"
    echo ""
    echo "Please check your DATABASE_URL in .env"
    exit 1
fi

# ============================================================
# Step 6: Apply migrations (idempotent)
# ============================================================
print_step 6 "Applying database migrations..."

echo "Running database migrations (safe to re-run)..."

MIGRATE_RESULT=$(python3 << 'EOF'
import os
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(".env"))

import psycopg2

conn = psycopg2.connect(os.environ["DATABASE_URL"])
conn.autocommit = True
cur = conn.cursor()

migrations = [
    "app/db/migrations/001_enable_extensions.sql",
    "app/db/migrations/002_core_schema.sql",
    "app/db/migrations/003_constraints_triggers.sql",
    "app/db/migrations/004_indexes.sql",
    "app/db/migrations/005_seed_policies.sql",
    "app/db/migrations/006_evidence_dedup.sql",
    "app/db/migrations/007_playbook_aging.sql",
]

for migration in migrations:
    try:
        with open(migration, "r") as f:
            sql = f.read()
        cur.execute(sql)
        print(f"OK:{migration}")
    except Exception as e:
        # Migrations are written to be idempotent (IF NOT EXISTS / OR REPLACE).
        # If a DB reports "already exists", we treat it as non-fatal and continue.
        error_str = str(e).replace("\n", " ")
        if "already exists" in error_str:
            print(f"SKIP:{migration}:{type(e).__name__} {error_str[:400]}")
        else:
            print(f"ERROR:{migration}:{type(e).__name__} {error_str[:400]}")

cur.close()
conn.close()
print("DONE")
EOF
)

# Parse migration results
MIGRATION_FAILED=0
while IFS= read -r line; do
    if [[ $line == OK:* ]]; then
        MIGRATION_FILE="${line#OK:}"
        print_success "Applied $(basename $MIGRATION_FILE)"
    elif [[ $line == SKIP:* ]]; then
        MIGRATION_FILE="${line#SKIP:}"
        print_warning "Skipped $(basename $MIGRATION_FILE) (already exists)"
    elif [[ $line == ERROR:* ]]; then
        print_error "$line"
        MIGRATION_FAILED=1
    fi
done <<< "$MIGRATE_RESULT"

if [ $MIGRATION_FAILED -eq 1 ]; then
    print_error "Some migrations failed. Check the errors above."
    exit 1
fi

print_success "Database migrations complete"

# ============================================================
# Step 7: Create directories
# ============================================================
print_step 7 "Creating required directories..."

mkdir -p var/evidence
print_success "Created var/evidence/"

# ============================================================
# Step 8: Verify installation
# ============================================================
print_step 8 "Verifying installation..."

VERIFY_RESULT=$(python3 << 'EOF'
import sys

# Test 1: FastAPI app loads
try:
    from app.main import app
    print("OK:FastAPI app loaded")
except Exception as e:
    print(f"ERROR:FastAPI app: {e}")
    sys.exit(1)

# Test 2: Database engine connects
try:
    from sqlalchemy import text
    from app.db.engine import SessionLocal
    session = SessionLocal()
    session.execute(text("SELECT 1"))
    session.close()
    print("OK:Database engine works")
except Exception as e:
    print(f"WARN:Database engine: {e}")

# Test 3: Core imports work
try:
    from app.agents.state_graph import AgentState, BeliefState, Posture
    from app.graph.traversal import traverse, get_subgraph
    from app.packets.builder import DecisionPacketBuilder
    print("OK:Core modules loaded")
except Exception as e:
    print(f"ERROR:Core modules: {e}")

print("DONE")
EOF
)

while IFS= read -r line; do
    if [[ $line == OK:* ]]; then
        MSG="${line#OK:}"
        print_success "$MSG"
    elif [[ $line == WARN:* ]]; then
        MSG="${line#WARN:}"
        print_warning "$MSG"
    elif [[ $line == ERROR:* ]]; then
        MSG="${line#ERROR:}"
        print_error "$MSG"
    fi
done <<< "$VERIFY_RESULT"

# ============================================================
# Complete!
# ============================================================
echo ""
echo "========================================"
echo -e "${GREEN}Setup complete!${NC}"
echo "========================================"
echo ""
echo "The system is ready. To start:"
echo ""
echo -e "  ${YELLOW}source .venv/bin/activate${NC}"
echo -e "  ${YELLOW}python -m uvicorn app.main:app --reload${NC}"
echo ""
echo "Or run both in one command:"
echo ""
echo -e "  ${YELLOW}source .venv/bin/activate && python -m uvicorn app.main:app --reload${NC}"
echo ""
echo "Then open: ${BLUE}http://localhost:8000${NC}"
echo ""
echo "API Documentation: ${BLUE}http://localhost:8000/docs${NC}"
echo ""

# Ask if user wants to start server now (skip in non-interactive mode)
if [ "$NO_PROMPT" = "false" ]; then
    read -p "Start the server now? [Y/n] " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]] || [[ -z $REPLY ]]; then
        echo ""
        echo -e "${GREEN}Starting server...${NC}"
        echo "Press Ctrl+C to stop"
        echo ""
        python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
    fi
fi
