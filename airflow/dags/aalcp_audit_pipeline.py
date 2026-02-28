"""
AALCP Audit Pipeline DAG

Orchestrates the end-to-end flow:
    run_simulation → extract_from_postgres → load_snowflake →
    data_quality_gates → refresh_cortex_search → rag_smoke_test

Schedule: @once (demo pipeline). Change to @daily for production use.
"""

import json
import os
import time
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

logger = logging.getLogger(__name__)

APP_BASE_URL = os.environ.get("APP_BASE_URL", "http://host.docker.internal:8000")
SCENARIOS = ["jfk_ground_stop", "ord_thunderstorm", "lax_clear_skies"]


# =============================================================================
# Task 1: Run simulation scenarios to generate decision packets
# =============================================================================

def _run_simulation(**kwargs):
    import requests

    # Try batch endpoint first (exists at simulation/api.py:153)
    url = f"{APP_BASE_URL}/simulation/run-batch"
    try:
        resp = requests.post(url, json={"scenario_ids": SCENARIOS}, timeout=300)
        if resp.status_code == 200:
            result = resp.json()
            logger.info("Batch simulation complete: %s", json.dumps(result, indent=2)[:500])
            return result
        elif resp.status_code == 404:
            logger.warning("run-batch returned 404, falling back to per-scenario loop")
        else:
            logger.warning("run-batch returned %d, falling back to per-scenario loop", resp.status_code)
    except requests.exceptions.ConnectionError:
        raise AirflowFailException(
            f"Cannot connect to app at {APP_BASE_URL}. "
            "Ensure the FastAPI app is running (make run)."
        )

    # Fallback: run scenarios individually
    results = []
    for scenario_id in SCENARIOS:
        url = f"{APP_BASE_URL}/simulation/run/{scenario_id}"
        resp = requests.post(url, timeout=120)
        if resp.status_code != 200:
            logger.error("Scenario %s failed: %d %s", scenario_id, resp.status_code, resp.text[:200])
            continue
        results.append(resp.json())
        logger.info("Scenario %s complete", scenario_id)

    if not results:
        raise AirflowFailException("All simulation scenarios failed")

    return {"results": results, "count": len(results)}


# =============================================================================
# Task 2: Extract packets + evidence from Postgres → staging JSONL
# =============================================================================

def _extract_from_postgres(**kwargs):
    import sys
    repo_root = os.environ.get("PYTHONPATH", "/opt/airflow/repo")
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

    from audit_warehouse.load import extract
    extract()
    logger.info("Extract complete")


# =============================================================================
# Task 3: Load staging JSONL → Snowflake RAW + refresh GOLD
# =============================================================================

def _load_snowflake(**kwargs):
    import sys
    repo_root = os.environ.get("PYTHONPATH", "/opt/airflow/repo")
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

    from audit_warehouse.load import load
    load()
    logger.info("Snowflake load complete")


# =============================================================================
# Task 4: Data quality gates (4 checks)
# =============================================================================

def _data_quality_gates(**kwargs):
    import snowflake.connector

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "AALCP_CORTEX_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "AALCP_DB"),
    )
    cur = conn.cursor()
    failures = []

    try:
        # Check 1: Freshness — latest packet created_at within 1 hour
        cur.execute("""
            SELECT MAX(created_at) FROM RAW.DECISION_PACKETS
        """)
        row = cur.fetchone()
        if row and row[0]:
            latest = row[0]
            if isinstance(latest, str):
                latest = datetime.fromisoformat(latest)
            age_hours = (datetime.now(latest.tzinfo or None) - latest).total_seconds() / 3600
            if age_hours > 1:
                failures.append(f"Freshness: latest packet is {age_hours:.1f}h old (threshold: 1h)")
            else:
                logger.info("Freshness OK: latest packet %.1fh old", age_hours)
        else:
            failures.append("Freshness: no packets found in RAW.DECISION_PACKETS")

        # Check 2: Minimum row counts (at least 1 row per RAW table)
        for table in ["RAW.DECISION_PACKETS", "RAW.EVIDENCE", "RAW.PACKET_DETAILS"]:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            if count < 1:
                failures.append(f"Row count: {table} has {count} rows (need >= 1)")
            else:
                logger.info("Row count OK: %s has %d rows", table, count)

        # Check 3: NULL rationale threshold (< 50%)
        cur.execute("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN rationale_text IS NULL OR rationale_text = '' THEN 1 ELSE 0 END) AS nulls
            FROM RAW.DECISION_PACKETS
        """)
        total, nulls = cur.fetchone()
        if total > 0:
            pct_null = (nulls / total) * 100
            if pct_null >= 50:
                failures.append(f"NULL rationale: {pct_null:.0f}% null (threshold: <50%)")
            else:
                logger.info("NULL rationale OK: %.0f%% null", pct_null)

        # Check 4: case_id uniqueness
        cur.execute("""
            SELECT case_id, COUNT(*) AS cnt
            FROM RAW.DECISION_PACKETS
            GROUP BY case_id
            HAVING COUNT(*) > 1
            LIMIT 5
        """)
        dupes = cur.fetchall()
        if dupes:
            failures.append(f"Uniqueness: {len(dupes)} duplicate case_ids found")
        else:
            logger.info("Uniqueness OK: no duplicate case_ids")

    finally:
        cur.close()
        conn.close()

    if failures:
        raise AirflowFailException(
            "Data quality gate failures:\n" + "\n".join(f"  - {f}" for f in failures)
        )

    logger.info("All data quality gates passed")


# =============================================================================
# Task 5: Refresh Cortex Search service
# =============================================================================

def _refresh_cortex_search(**kwargs):
    import snowflake.connector
    from pathlib import Path

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "AALCP_CORTEX_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "AALCP_DB"),
    )
    cur = conn.cursor()
    try:
        sql_path = Path("/opt/airflow/repo/audit_warehouse/sql/03_cortex_search.sql")
        if not sql_path.exists():
            raise AirflowFailException(f"SQL file not found: {sql_path}")

        sql = sql_path.read_text()
        for statement in sql.split(";"):
            stmt = statement.strip()
            if stmt and not stmt.startswith("--"):
                cur.execute(stmt)

        logger.info("Cortex Search service refreshed")
    finally:
        cur.close()
        conn.close()


# =============================================================================
# Task 6: RAG smoke test (5 queries with retry for Cortex warm-up)
# =============================================================================

def _rag_smoke_test(**kwargs):
    import requests

    test_questions = [
        "Which airports had HOLD posture and why?",
        "What contradictions were detected in recent cases?",
        "Which cases had SLA breaches imminent?",
        "What policies were applied to ESCALATE decisions?",
        "How many shipments were affected at JFK?",
        "Which specific policy blocked the ground stop case?",
        "What premium service shipments had imminent SLA breach?",
    ]

    url = f"{APP_BASE_URL}/rag/query"
    max_retries = 6
    retry_delay = 10

    for i, question in enumerate(test_questions, 1):
        success = False
        for attempt in range(1, max_retries + 1):
            try:
                resp = requests.post(
                    url,
                    json={"question": question, "top_k": 3},
                    timeout=60,
                )
                if resp.status_code != 200:
                    logger.warning(
                        "Q%d attempt %d: HTTP %d - %s",
                        i, attempt, resp.status_code, resp.text[:200],
                    )
                    time.sleep(retry_delay)
                    continue

                result = resp.json()
                citations = result.get("citations", [])
                answer = result.get("answer", "")

                if citations and answer:
                    logger.info("Q%d passed: %d citations, answer length %d", i, len(citations), len(answer))
                    success = True
                    break
                else:
                    logger.warning(
                        "Q%d attempt %d: empty citations/answer, retrying in %ds",
                        i, attempt, retry_delay,
                    )
                    time.sleep(retry_delay)

            except requests.exceptions.ConnectionError:
                raise AirflowFailException(
                    f"Cannot connect to app at {APP_BASE_URL}. Ensure the FastAPI app is running."
                )
            except Exception as e:
                logger.warning("Q%d attempt %d: %s", i, attempt, e)
                time.sleep(retry_delay)

        if not success:
            raise AirflowFailException(
                f"RAG smoke test failed for Q{i}: '{question}' "
                f"— no citations after {max_retries} retries"
            )

    logger.info("All %d RAG smoke test queries passed", len(test_questions))


# =============================================================================
# DAG definition
# =============================================================================

default_args = {
    "owner": "aalcp",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="aalcp_audit_pipeline",
    default_args=default_args,
    description="Extract decision packets from Postgres → load Snowflake → refresh Cortex → smoke test RAG",
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["aalcp", "audit", "snowflake", "cortex"],
) as dag:

    t1 = PythonOperator(task_id="run_simulation", python_callable=_run_simulation)
    t2 = PythonOperator(task_id="extract_from_postgres", python_callable=_extract_from_postgres)
    t3 = PythonOperator(task_id="load_snowflake", python_callable=_load_snowflake)
    t4 = PythonOperator(task_id="data_quality_gates", python_callable=_data_quality_gates)
    t5 = PythonOperator(task_id="refresh_cortex_search", python_callable=_refresh_cortex_search)
    t6 = PythonOperator(task_id="rag_smoke_test", python_callable=_rag_smoke_test)

    t1 >> t2 >> t3 >> t4 >> t5 >> t6
