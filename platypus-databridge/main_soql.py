from __future__ import annotations

import concurrent.futures
import os
import time
from datetime import datetime as dt
from datetime import timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, TypedDict

import duckdb
from dotenv import load_dotenv
from platypus.salesforce_client import SalesforceQueryExecutor

import soql.soql_queries as soql_queries

# ========================= #
# Configuration             #
# ========================= #

# Load env from .env in the current working directory
load_dotenv(dotenv_path=r".env", override=True)

DUCKDB_FILE: str = r"db\query_log.duckdb"
DEFAULT_FREQUENCY: str = "daily"
DEFAULT_FORMAT: str = "parquet"
MAX_WORKERS: int = 5


# # Ensures you have the your Salesforce credentials


def ensure_salesforce_env() -> None:
    required = [
        "SALESFORCE_USERNAME",
        "SALESFORCE_PASSWORD",
        "SALESFORCE_TOKEN",  # remove if your executor does not need it
    ]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing Salesforce env vars: {', '.join(missing)}")


# ============================================================================================ #
# DuckDB Utilities - Used to initialize, update, log query stats, and determine run executions #
# ============================================================================================ #


def get_user() -> str:
    return os.getlogin()


def initialize_duckdb_query_log() -> None:
    with duckdb.connect(DUCKDB_FILE) as con:
        con.execute("CREATE SEQUENCE IF NOT EXISTS query_log_seq")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS query_log (
                id BIGINT PRIMARY KEY DEFAULT nextval('query_log_seq'),
                query_name TEXT,
                last_run TIMESTAMP,
                frequency TEXT,
                status TEXT,
                execution_time_seconds DOUBLE,
                error_message TEXT,
                user_id TEXT
            )
            """
        )


def initialize_duckdb_query_performance_log() -> None:
    with duckdb.connect(DUCKDB_FILE) as con:
        con.execute("CREATE SEQUENCE IF NOT EXISTS performance_log_seq")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS query_performance_log (
                id BIGINT PRIMARY KEY DEFAULT nextval('performance_log_seq'),
                query_name TEXT,
                execution_time_seconds DOUBLE,
                last_run TIMESTAMP,
                user_id TEXT
            )
            """
        )


LastRunInfo = Tuple[dt, str, str]


def get_last_run_info(query_name: str) -> Optional[LastRunInfo]:
    with duckdb.connect(DUCKDB_FILE) as con:
        row = con.execute(
            """
            SELECT last_run, frequency, status
            FROM query_log
            WHERE query_name = ?
            ORDER BY last_run DESC
            LIMIT 1
            """,
            (query_name,),
        ).fetchone()
    return row


def should_run_query(query_name: str, frequency: str, force_run: bool = False) -> bool:
    if force_run:
        return True

    info = get_last_run_info(query_name)
    if info is None:
        return True

    last_run, _, status = info
    if last_run is None or (status and status.lower() == "failed"):
        return True

    window = {
        "now": timedelta(seconds=1),
        "daily": timedelta(hours=12),
        "weekly": timedelta(weeks=1),
        "quarterly": timedelta(days=120),
    }.get(frequency, timedelta(weeks=1))

    return dt.now() - last_run >= window


def update_query_log(
    query_name: str,
    frequency: str,
    status: str,
    execution_time: Optional[float] = None,
    error_message: Optional[str] = None,
) -> None:
    user_id = get_user()
    with duckdb.connect(DUCKDB_FILE) as con:
        con.execute(
            """
            INSERT INTO query_log (
                query_name, last_run, frequency, status,
                execution_time_seconds, error_message, user_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                query_name,
                dt.now().strftime("%Y-%m-%d %H:%M:%S"),
                frequency,
                status,
                execution_time,
                error_message,
                user_id,
            ),
        )


def log_query_performance(query_name: str, execution_time: Optional[float]) -> None:
    user_id = get_user()
    with duckdb.connect(DUCKDB_FILE) as con:
        con.execute(
            """
            INSERT INTO query_performance_log (query_name, execution_time_seconds, last_run, user_id)
            VALUES (?, ?, ?, ?)
            """,
            (query_name, execution_time, dt.now().strftime("%Y-%m-%d %H:%M:%S"), user_id),
        )


# ===================================================================================================
# Data Shapes
# Used to construct the output configs that get passed to build_registry().
# build_registry() then passes them to DremioQueryExecuter in the proper "output_config" format.
# ===================================================================================================


class OutputConfig(TypedDict):
    output_format: str
    output_filename: str


class SoqlRecord(TypedDict, total=False):
    soql: str
    frequency: str
    output_config: OutputConfig
    force_run: bool


# =================================================== #
# Registry                                            #
# Define the default SOQL set. You can add more here. #
# =================================================== #

SOQL_SOURCES: List[Tuple[str, str]] = [
    ("employee.soql", soql_queries.employee_soql),
    ("user_role.soql", soql_queries.user_role_soql),
    ("user.soql", soql_queries.user_soql),
    ("opportunity.soql", soql_queries.opportunity_soql),
    ("campaign_member.soql", soql_queries.campaign_member_soql),
]

# ================================================================================================================================== #
# Only specify different frequencies other than the default frequency here. Everything else gets inferred through DEFAULT_FREQUENCY. #
# Set force_run to True when you need to re-run or run a query ad-hoc.                                                               #
# ================================================================================================================================== #
OVERRIDES: Dict[str, Dict[str, object]] = {
    "campaign_member.soql": {"frequency": "weekly"},
}


def build_soql_registry() -> Dict[str, SoqlRecord]:
    registry: Dict[str, SoqlRecord] = {}
    for name, soql in SOQL_SOURCES:
        meta = OVERRIDES.get(name, {})
        frequency = str(meta.get("frequency", DEFAULT_FREQUENCY))
        output_format = str(meta.get("output_format", DEFAULT_FORMAT))
        output_filename = str(meta.get("output_filename", Path(name).stem))
        force_run = bool(meta.get("force_run", False))

        registry[name] = {
            "soql": soql,
            "frequency": frequency,
            "output_config": {"output_format": output_format, "output_filename": output_filename},
            "force_run": force_run,
        }
    return registry


# =========================
# Executor logic
# =========================


def execute_soql_query(query_name: str, rec: SoqlRecord) -> None:
    ensure_salesforce_env()
    start = time.time()
    executor = SalesforceQueryExecutor()
    try:
        executor.execute_queries_with_configs(
            queries=[record["soql"]],
            output_configs=[record["output_config"]],
        )
        elapsed = round(time.time() - start, 2)
        update_query_log(query_name, record["frequency"], "success", elapsed)
        log_query_performance(query_name, elapsed)
        print(f"✅ Query executed successfully: {query_name} in {elapsed} seconds")

    except Exception as exc:
        msg = str(exc)
        update_query_log(query_name, record["frequency"], "failed", None, msg)
        print(f"❌ Error executing {query_name}: {msg}")


# =========================
# Ad-hoc helpers
# =========================


def run_single_soql(query_name: str, force_run: bool = False) -> None:
    initialize_duckdb_query_log()
    initialize_duckdb_query_performance_log()

    registry = build_soql_registry()
    if query_name not in registry:
        print(f"Not found: {query_name}")
        return

    record = registry[query_name]
    if force_run:
        record["force_run"] = True

    print(f"Running single SOQL: {query_name} (force_run={force_run})")
    execute_soql_query(query_name, record)


def run_multiple_soql(*query_names: str, force_run: bool = False) -> None:
    initialize_duckdb_query_log()
    initialize_duckdb_query_performance_log()

    registry = build_soql_registry()
    for query_name in query_names:
        if query_name not in registry:
            print(f"❌ Query not found: {query_name}")
            continue
        record = registry[query_name]
        if force_run:
            record["force_run"] = True
        print(f"Running SOQL: {name} (force_run={force_run})")
        execute_soql_query(name, record)


# =========================
# Main orchestration
# =========================


def main() -> None:
    initialize_duckdb_query_log()
    initialize_duckdb_query_performance_log()

    registry = build_soql_registry()

    to_run = {
        name: record
        for name, record in registry.items()
        if should_run_query(name, record["frequency"], record.get("force_run", False))
    }

    if not to_run:
        print("All Salesforce SOQL tasks are already up to date")
        return

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(execute_soql_query, n, r): n for n, r in to_run.items()}
        for f in concurrent.futures.as_completed(futures):
            f.result()

    print("Finished Salesforce SOQL batch")
