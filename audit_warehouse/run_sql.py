"""
Run Snowflake SQL files via Python (no snowsql needed).

Usage:
    python -m audit_warehouse.run_sql audit_warehouse/sql/01_schema.sql
    python -m audit_warehouse.run_sql audit_warehouse/sql/01_schema.sql audit_warehouse/sql/03_cortex_search.sql
    python -m audit_warehouse.run_sql --all   # runs 01, 02, 03, 04 in order
"""

import sys
import os
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL_DIR = Path(__file__).resolve().parent / "sql"

ALL_SCRIPTS = [
    SQL_DIR / "01_schema.sql",
    SQL_DIR / "02_gold_transforms.sql",
    SQL_DIR / "03_cortex_search.sql",
    SQL_DIR / "04_validate.sql",
]


def run_sql_file(sql_path: Path):
    """Execute a SQL file against Snowflake, statement by statement."""
    import snowflake.connector

    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        logger.error("Missing env vars: %s. Set them in .env", ", ".join(missing))
        sys.exit(1)

    if not sql_path.exists():
        logger.error("File not found: %s", sql_path)
        sys.exit(1)

    sql = sql_path.read_text()
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "AALCP_CORTEX_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "AALCP_DB"),
        login_timeout=30,
        network_timeout=60,
    )
    cur = conn.cursor()

    try:
        executed = 0
        for statement in sql.split(";"):
            stmt = statement.strip()
            if stmt and not stmt.startswith("--"):
                try:
                    cur.execute(stmt)
                    executed += 1
                except Exception as e:
                    # Idempotent: skip "already exists" errors
                    err = str(e)
                    if "already exists" in err.lower():
                        logger.info("Skipped (already exists): %.80s...", stmt.split("\n")[0])
                    else:
                        logger.error("Failed: %s\n  SQL: %.120s...", e, stmt.split("\n")[0])
                        raise
        logger.info("Executed %d statements from %s", executed, sql_path.name)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    # Load .env
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    if len(sys.argv) < 2:
        print(f"Usage: python -m audit_warehouse.run_sql <file.sql> [file2.sql ...]")
        print(f"       python -m audit_warehouse.run_sql --all")
        sys.exit(1)

    if sys.argv[1] == "--all":
        files = ALL_SCRIPTS
    else:
        files = [Path(f) for f in sys.argv[1:]]

    for f in files:
        logger.info("Running %s ...", f.name)
        run_sql_file(f)

    logger.info("Done.")
