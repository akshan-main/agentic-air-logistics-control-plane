# Archived README (v1)

This file is kept as a snapshot of an earlier draft. Use `README.md` for the current project overview and run instructions.

---

# Forwarder Exception OS

Air freight Gateway Posture Directive system that continuously ingests real disruption signals and outputs governed operational decisions.

## Quick Start

```bash
# 1. Run setup
./setup.sh

# 2. Activate environment
source .venv/bin/activate

# 3. Edit database credentials
nano .env

# 4. Run server
python -m uvicorn app.main:app --reload

# 5. Open browser
open http://localhost:8000
```

## Database Setup

You need PostgreSQL with pgvector. Choose one option:

### Option A: Supabase (Easiest - Free)

1. Go to [supabase.com](https://supabase.com) and create a project
2. Go to Settings > Database > Connection string
3. Copy the URI and add to `.env`:
   ```
   DATABASE_URL=postgresql://postgres:[PASSWORD]@db.[PROJECT].supabase.co:5432/postgres
   ```
4. Run SQL Editor and execute the migration files in order:
   - `app/db/migrations/001_enable_extensions.sql`
   - `app/db/migrations/002_core_schema.sql`
   - `app/db/migrations/003_constraints_triggers.sql`
   - `app/db/migrations/004_indexes.sql`

### Option B: Neon.tech (Free)

1. Go to [neon.tech](https://neon.tech) and create a database
2. Enable pgvector extension in dashboard
3. Copy connection string to `.env`
4. Run migrations via their SQL editor

### Option C: Local Docker

```bash
docker compose up -d
make migrate
```

### Option D: Local Postgres (Homebrew)

```bash
brew install postgresql@16 pgvector
brew services start postgresql@16
createdb exception_os
psql exception_os -f app/db/migrations/001_enable_extensions.sql
psql exception_os -f app/db/migrations/002_core_schema.sql
psql exception_os -f app/db/migrations/003_constraints_triggers.sql
psql exception_os -f app/db/migrations/004_indexes.sql
```

## Testing

```bash
# Run all tests (requires database)
pytest tests/ -v

# Run tests without database
pytest tests/test_security.py -v
pytest tests/test_agent_non_workflow.py -v -k "deterministic"

# Run with coverage
pytest tests/ --cov=app --cov-report=html
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/ingest/airport/{icao}` | POST | Ingest signals for airport |
| `/cases` | POST | Create exception case |
| `/cases/{id}/run` | POST | Run agent on case |
| `/packets/{id}` | GET | Get decision packet |
| `/playbooks/similar` | GET | Find similar playbooks |

## Architecture

```
app/
├── api/          # FastAPI routes
├── agents/       # State machine agent
│   ├── roles/    # Investigator, Critic, etc.
│   ├── planner/  # Beam search planner
│   └── guardrails/
├── db/           # Database migrations
├── evidence/     # Immutable evidence store
├── governance/   # Approval workflows
├── graph/        # Bi-temporal context graph
├── ingestion/    # FAA, Weather, NWS, OpenSky
├── packets/      # Decision packets
├── policy/       # Policy engine
├── replay/       # Playbook learning
├── signals/      # Derived signals
└── ui/           # Minimal web UI
```

## Key Features

- **Bi-temporal database**: Track both event time and ingestion time
- **Evidence binding**: All FACT claims must link to evidence
- **12-state agent**: Deterministic state machine, not a ReAct loop
- **Posture directives**: ACCEPT, RESTRICT, HOLD, ESCALATE
- **PDL metric**: Posture Decision Latency tracking
- **Replay learning**: Playbook reuse after 3 similar cases
