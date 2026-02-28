# Agentic Air Logistics Control Plane

A multi-agent system for air freight gateway posture decisions. It ingests live disruption signals (FAA ground stops, METAR/TAF weather, NWS alerts, OpenSky ADS-B traffic), stores every raw payload as immutable SHA-256-addressed evidence, builds a bi-temporal context graph, and runs specialist agents through a deterministic state machine to produce a governed **decision packet** — the canonical audit artifact that says what posture to take, why, and what evidence backs it.

The four postures: **ACCEPT** (open for bookings), **RESTRICT** (accept with constraints), **HOLD** (pause until evidence clears), **ESCALATE** (route to a duty manager).

![Decision packet](docs/images/packet.svg)

## What makes this different

**The system knows what it doesn't know.** When a source fetch fails (OpenSky timeout, NWS 503), it doesn't silently degrade — it records a `missing_evidence_request` with a criticality level. BLOCKING missing evidence stops the entire case until resolved. The packet shows exactly what's missing and why.

**Evidence binding is enforced at the database level.** A Postgres trigger rejects any attempt to promote a claim or edge to FACT without linked evidence. You literally cannot have an unsupported fact in the graph.

**Orchestration is a state machine, not a ReAct loop.** Six specialist agents (Investigator, RiskQuant, Critic, PolicyJudge, Comms, Executor) run in a fixed sequence with deterministic transitions. The planner uses beam search (width 4, depth 4) to score candidate actions by information gain and cost — no LLM calls in the planning step.

**Actions go through governance.** Every proposed action follows a lifecycle: PROPOSED → PENDING_APPROVAL → APPROVED → EXECUTING → COMPLETED (or FAILED → ROLLED_BACK). The policy engine evaluates 13 built-in rules covering evidence requirements, approval thresholds, risk-posture mappings, and operational constraints. If a shipment action (hold cargo, rebook flight, etc.) is proposed without booking evidence, the system blocks the case and creates a missing evidence request rather than executing blindly.

**The graph is bi-temporal.** Every edge and claim tracks both event time (when it happened in the real world) and ingest time (when the system learned about it). This means you can query the graph "as of" any point in time for audit and replay.

**Playbooks learn and decay.** Resolved cases get mined for reusable patterns. These playbooks age with domain-specific half-lives — weather patterns decay in 30 days, operational patterns in 90, regulatory patterns in 180. When policies change, the system detects drift by comparing policy snapshots using Jaccard similarity, automatically reducing the relevance of stale playbooks.

## How it works

The runtime pipeline is straightforward:

```
INIT → INVESTIGATE → QUANTIFY_RISK → CRITIQUE → EVALUATE_POLICY → PLAN_ACTIONS → EXECUTE → COMPLETE
```

Under the hood, a more detailed 12-state decomposition handles the mechanics:

![Pipeline](docs/images/pipeline.svg)

1. Ingest all five sources in parallel via ThreadPoolExecutor
2. Store raw bytes as immutable evidence (deduplicated by source + ref + SHA-256)
3. Derive graph edges from evidence (FAA disruption, weather risk, NWS alert, movement collapse) — each edge is bound to the evidence that produced it
4. Detect contradictions — the system catches things like "FAA says normal operations but ADS-B shows traffic collapsed to 30% of baseline"
5. Run the specialist agents: Investigator gathers evidence and creates claims, RiskQuant assesses risk with an LLM, Critic challenges evidence quality, PolicyJudge evaluates all 13 policies
6. Plan actions via beam search, then execute approved ones through the governance state machine
7. Package everything into a decision packet and persist it
8. Mine playbooks from the resolved case for future reference

Webhooks fire on posture changes, action executions, case resolution, and imminent SLA breaches. Registration includes SSRF protection (private IP ranges are blocked).

## Getting started

You need Python 3.11+ and Postgres with pgvector. Any Postgres will work — local, Docker, Supabase, Neon.

```bash
cp .env.example .env          # set DATABASE_URL, optionally LLM keys
./setup.sh                     # creates venv, installs deps, runs migrations
make run                       # starts FastAPI on :8000
```

Open `http://localhost:8000/ui/`.

The setup script is idempotent — re-run it after pulling to apply new migrations. If you'd rather use Docker for Postgres: `make up` first, then `./setup.sh && make run`.

For full agent runs (not just ingestion/graph), you need an LLM provider:

```bash
# in .env
LLM_PROVIDER=openai
OPENAI_API_KEY=sk-...
# or: LLM_PROVIDER=anthropic + ANTHROPIC_API_KEY
```

## Try it out

**Quick demo** — ingest real signals for JFK, create a case, run the agent:

```bash
curl -X POST localhost:8000/ingest/airport/KJFK
curl -X POST localhost:8000/cases \
  -H "Content-Type: application/json" \
  -d '{"case_type":"AIRPORT_DISRUPTION","scope":{"airport":"KJFK"}}'
curl -X POST localhost:8000/cases/<case_id>/run
curl localhost:8000/packets/<case_id>
```

**Scenario demo** — run a pre-built scenario that seeds operational data (flights, shipments, bookings) and runs the full pipeline including cascade analysis:

```bash
curl localhost:8000/simulation/scenarios                           # list scenarios
curl -X POST localhost:8000/simulation/run/jfk_ground_stop         # run one
curl -X POST localhost:8000/simulation/run-batch \
  -H "Content-Type: application/json" \
  -d '{"scenario_ids":["jfk_ground_stop","ord_thunderstorm","lax_clear_skies"]}'
```

**UI demo** — the web UI at `/ui/` has three paths:
1. *Posture-only*: select airport → Ingest Signals → Create Case → Run Agent
2. *With cascade*: select airport → Refresh Ops Graph → Create Case → Run Agent (shows flights/shipments/bookings)
3. *Scenario*: hit the simulation API — everything is automated

The simulation uses a small set of major airports to keep things lightweight. `Refresh Ops Graph` clears and re-seeds so SLA times stay anchored near "now".

## Under the hood

### Agents and their support systems

The six agents (Investigator, RiskQuant, Critic, PolicyJudge, Comms, Executor) are backed by several subsystems:

**Guardrails** (`app/agents/guardrails/`) — hard-fail gates that block unsafe operations. EvidenceBindingGate prevents promoting claims to FACT without evidence. NoShipmentActionWithoutBookingGate blocks cargo operations without booking proof (creates a MissingEvidenceRequest and sets the case to BLOCKED). NonWorkflowGate verifies that different cases actually produce different reasoning paths, not just a replayed script.

**Memory** (`app/agents/memory/`) — episodic memory recalls similar past cases (filtered by type and airport) with their trace events and outcomes. Semantic memory retrieves playbooks ranked by `success_rate * decay_factor * policy_alignment * confidence`, detecting policy drift via Jaccard similarity of policy text snapshots. Working memory holds the current run's accumulated state.

**Planner** (`app/agents/planner/`) — deterministic beam search that scores investigation actions by `information_gain - cost` and intervention actions by `action_value - cost - risk_penalty`, using pre-computed lookup tables. No LLM involved.

### Governance

Every action goes through `app/governance/`:

- **State machine**: 7 states (PROPOSED → PENDING_APPROVAL → APPROVED → EXECUTING → COMPLETED/FAILED → ROLLED_BACK) with enforced transitions and trace logging
- **Approvals**: request/approve/reject flow; cases auto-resolve when all actions reach terminal states
- **Rollback**: action-specific rollback logic for SET_POSTURE, PUBLISH_GATEWAY_ADVISORY, UPDATE_BOOKING_RULES, TRIGGER_REEVALUATION, and HOLD_CARGO

### Policies

13 built-in policies in `app/policy/builtin_policies.py`, seeded to Postgres via migration 005:

- *Evidence*: contradiction resolution required, minimum evidence threshold, shipment booking evidence required, contradiction + stale data handling
- *Approval*: high/critical risk requires approval, premium SLA actions require approval, cost threshold approval
- *Risk-posture*: critical risk cannot be ACCEPT, high risk recommends HOLD
- *Operational*: low risk allows ACCEPT, medium allows RESTRICT, weather evidence required, IFR conditions require review

The PolicyJudge evaluates all active policies. A post-LLM safety override prevents false BLOCK verdicts when no shipment actions are actually proposed (the LLM sometimes gets this wrong).

### Signal detection

`app/signals/` detects four types of contradictions across sources: FAA-weather mismatch (normal ops vs IFR/LIFR), FAA-movement mismatch (normal ops vs traffic collapse), weather-movement mismatch (VFR vs aircraft collapse), and stale FAA data. Movement collapse computes percent-of-baseline against per-airport reference values. The derivation pipeline turns raw evidence into typed graph edges (FAA disruption, weather risk, NWS alert, movement collapse).

### Playbook aging

Resolved cases get mined into playbooks (`app/replay/`). Relevance decays exponentially: weather playbooks have a 30-day half-life, operational 90 days, customs/regulatory 180 days. Formula: `decay = 0.5^(age_days / half_life)`. When policies change between when a playbook was created and now, alignment scoring via Jaccard similarity of policy text hashes reduces the playbook's weight. Low sample counts also incur a confidence penalty.

### Webhooks

`app/webhooks/` provides event-driven HTTP POST notifications for four events: POSTURE_CHANGE, ACTION_EXECUTED, CASE_RESOLVED, SLA_BREACH_IMMINENT. Standardized payload format with delivery logging. Webhook registration validates URLs and blocks private IP ranges (SSRF protection).

## Testing

```bash
# Fast (no DB required)
pytest tests/test_security.py tests/test_agent_non_workflow.py -v -m "not requires_db"

# Full suite (needs Postgres)
pytest tests/ -v
```

## Audit warehouse and Cortex RAG

Snowflake is the analytics and audit layer — Postgres stays as the operational DB.

```
Postgres (operational) → Airflow → Snowflake (audit) → Cortex Search → /rag/query
```

Decision packets are extracted from Postgres, flattened into JSONL, loaded into Snowflake RAW tables via MERGE (idempotent by primary key), then aggregated into GOLD tables (posture daily, contradictions daily, evidence coverage daily).

**Two-tier Cortex Search** indexes packets at two granularity levels:

- **PACKET_SEARCH** — packet-level rationale and cascade impact text. Good for broad questions like "Which airports had HOLD posture and why?"
- **DETAIL_SEARCH** — individual policy evaluations, shipments, contradictions, claims, and actions exploded into `RAW.PACKET_DETAILS` (one row per sub-component). Good for precise questions like "Which policy blocked the JFK case?" or "Which premium shipments had imminent SLA breaches?"

`POST /rag/query` queries both tiers, merges and deduplicates results, then generates a grounded answer via AI_COMPLETE. Citations are deterministic (case_ids from search results, not parsed from LLM output). The response includes both `snippets` (packet-level) and `details` (granular sub-documents with `detail_type` labels).

### Snowflake setup

1. Create an account in a Cortex-supported region (US East/West, EU)
2. Create warehouse: `CREATE WAREHOUSE AALCP_CORTEX_WH WAREHOUSE_SIZE='X-SMALL' AUTO_SUSPEND=60 AUTO_RESUME=TRUE;`
3. Run `audit_warehouse/sql/01_schema.sql` in a Snowflake Worksheet
4. Add credentials to `.env`: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`

### Running the pipeline

```bash
python -m audit_warehouse.load preflight   # check Postgres + Snowflake connectivity
python -m audit_warehouse.load run         # extract + load with structured logging
python -m audit_warehouse.load verify      # 8 data quality checks (row counts incl. PACKET_DETAILS,
                                           # freshness, NULL rationale %, uniqueness, GOLD tables,
                                           # watermarks, both Cortex Search services reachable)
```

Each run writes structured JSON metrics to `audit_warehouse/logs/run_<id>.jsonl`. The Airflow DAG (`airflow/dags/aalcp_audit_pipeline.py`) automates the full cycle: simulate → extract → load → quality gates → refresh both Cortex Search services → smoke test RAG (7 queries including granular policy and shipment questions).

### Design notes

Packets are immutable after resolution — the case table has no `updated_at` by design. Watermarking on `created_at` is correct because resolved cases never get reprocessed; re-evaluation creates a new case. Snowflake PRIMARY KEY declarations are not enforced — idempotency comes from the MERGE ON clause, not from Snowflake constraints. The RAG prompt instructs the model to respond with "Insufficient evidence in retrieved packets" when snippets don't support an answer. Granular details use a synthetic primary key (`case_id::detail_type::seq`) for deterministic MERGE.

## API reference

| Endpoint | What it does |
|----------|-------------|
| `POST /cases` | Create a case |
| `POST /cases/{id}/run` | Run agent orchestration |
| `GET /cases/{id}` | Case status |
| `POST /ingest/airport/{icao}` | Ingest all 5 sources |
| `GET /packets/{case_id}` | Full decision packet |
| `GET /packets/{case_id}/summary` | Packet summary |
| `GET /packets` | List packets |
| `POST /graph/bitemporal/beliefs` | Point-in-time graph query |
| `GET /graph/cascade/{icao}` | Cascade impact analysis |
| `POST /sandbox/explore/{icao}` | Full pipeline with real data |
| `GET /playbooks` | Playbooks with aging scores |
| `POST /webhooks/register` | Register webhook |
| `GET /webhooks/deliveries` | Delivery log |
| `POST /simulation/run/{scenario_id}` | Run scenario |
| `POST /simulation/run-batch` | Run multiple scenarios |
| `POST /simulation/seed/airport/{icao}` | Seed operational graph |
| `POST /rag/query` | Cortex Search + AI_COMPLETE Q&A |

## Repo map

```
app/
  api/              routes: cases, graph, ingest, packets, playbooks, webhooks, sandbox, RAG
  agents/
    orchestrator.py  deterministic state machine
    roles/           investigator, risk_quant, critic, policy_judge, comms, executor
    guardrails/      evidence binding, booking gate, missing evidence blocker
    memory/          episodic, semantic, working
    planner/         beam search + action library
  governance/        approval workflows, action state machine, rollback
  policy/            engine + 13 built-in policies
  signals/           contradiction, movement collapse, weather, congestion, derivation
  graph/             bi-temporal store, traversal, retrieval, similarity, visibility
  ingestion/         FAA, METAR/TAF, NWS, OpenSky clients
  evidence/          immutable store, hashing, excerpt redaction
  packets/           decision packet builder + models
  replay/            playbook mining, evaluation, aging
  webhooks/          registry, executor, SSRF protection
  db/migrations/     7 migrations (schema, triggers, indexes, policies, dedup, aging)
audit_warehouse/     Snowflake SQL (4 scripts), extract/load pipeline (packets + details), run logs
rag/                 Two-tier Cortex Search (packet + detail) + AI_COMPLETE module
airflow/             DAG (6 tasks, 7 smoke test queries), Dockerfile, docker-compose
simulation/          scenarios, operational data, graph seeder, runner
```
