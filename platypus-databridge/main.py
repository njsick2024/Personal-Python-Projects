# %%
from __future__ import annotations

import argparse
import concurrent.futures
import os
import sys
import time
from datetime import datetime as dt
from datetime import timedelta
from pathlib import Path
from typing import Dict, Optional, Tuple, TypedDict

import duckdb
from dotenv import load_dotenv
from platypus.dremio_client.query_executor import DremioQueryExecutor

from parquet_file_mover import ParquetFileManager
from utils import get_sql_queries

# ========================= #
# Configuration             #
# ========================= #

# loads .env from specified location. Default location is the current working directory
load_dotenv(dotenv_path=r".env", override=True)


# Ensures you have your dremio endpoint and password defined properly.
def ensure_dremio_env() -> None:
    required = ["DREMIO_ENDPOINT", "DREMIO_PASSWORD"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing required Dremio env vars: {', '.join(missing)}")


DUCKDB_FILE: str = r"db\query_log.duckdb"
DEFAULT_FREQUENCY: str = "daily"
DEFAULT_FORMAT: str = "parquet"

# ================================================================================================================================== #
# Only specify different frequencies other than the default frequency here. Everything else gets inferred through DEFAULT_FREQUENCY. #
# Set force_run to True when you need to re-run or run a query ad-hoc.                                                               #
# ================================================================================================================================== #

OVERRIDES: Dict[str, Dict[str, object]] = {
    # "account_summary.sql": {"force_run": True},
}

FILE_MANAGER = ParquetFileManager(
    new_files_dir=Path("./output"),
    core_files_dir=Path("parquet_files"),
    review_dir=Path("./output_review"),
    dry_run=False,
    size_threshold=0.99,
    min_size_bytes=10 * 1024,
    overrides={
        #####? Overdride files that don't need to be checked for size discrepencies
        # "tiny_reference_table.parquet": {"ignore_min_size": True},
        # "bad_but_keep.parquet": {"ignore_ratio": True},
        # "force_this_to_core.parquet": {"force_core": True},
        # "quarantine_this.parquet": {"force_review": True},
    },
    open_explorer_on_review=True,
)

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
    # We only care about the first and last variable "_" is a throw away value.
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
            query="""
            INSERT INTO query_log (
                query_name, last_run, frequency, status,
                execution_time_seconds, error_message, user_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            parameters=(
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
            query="""
            INSERT INTO query_performance_log (query_name, execution_time_seconds, last_run, user_id)
            VALUES (?, ?, ?, ?)
            """,
            parameters=(query_name, execution_time, dt.now().strftime("%Y-%m-%d %H:%M:%S"), user_id),
        )


# initialize_duckdb_query_log()
# initialize_duckdb_query_performance_log()


# ===================================================================================================
# Data Shapes
# Used to construct the output configs that get passed to build_registry().
# build_registry() then passes them to DremioQueryExecuter in the proper "output_config" format.
# ===================================================================================================


class OutputConfig(TypedDict):
    output_format: str
    output_filename: str


class QueryRecord(TypedDict, total=False):
    sql: str
    frequency: str
    output_config: OutputConfig
    force_run: bool


# ========================================================================================================================================================================================================
# Registry
# Scans the sql folder, loads each .sql file’s contents, and creates one dictionary entry per file containing its SQL text, schedule frequency, output configuration, and optional force_run flag.
# It applies default values for all fields, then overrides them if the file is listed in the OVERRIDES dictionary.
# This single registry replaces multiple separate mappings, making it easier to manage schedules, output settings, reruns, or ad_hoc
# ========================================================================================================================================================================================================


def build_registry(sql_dir: str = "sql") -> Dict[str, QueryRecord]:
    sql_queries = get_sql_queries(directory=sql_dir)  # {filename.sql: sql text}
    registry: Dict[str, QueryRecord] = {}

    for name, sql_text in sql_queries.items():
        meta = OVERRIDES.get(name, {})
        frequency = str(meta.get("frequency", DEFAULT_FREQUENCY))
        output_format = str(meta.get("output_format", DEFAULT_FORMAT))
        output_filename = str(meta.get("output_filename", Path(name).stem))
        force_run = bool(meta.get("force_run", False))

        registry[name] = {
            "sql": sql_text,
            "frequency": frequency,
            "output_config": {"output_format": output_format, "output_filename": output_filename},
            "force_run": force_run,
        }

    return registry


# =================================================================================
# Executor Logic - Gets called by:
# with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
#     futures = {pool.submit(execute_query, n, r): n for n, r in to_run.items()}
#     for f in concurrent.futures.as_completed(futures):
#         f.result()
#
# Two helper functions to run ad-hoc and force the run
#
# run_single_query("bc_listing.sql", force_run=True)
# run_multiple_queries("bc_listing.sql","cost_center.sql", force_run=True)
# =================================================================================


def execute_query(query_name: str, rec: QueryRecord) -> None:
    ensure_dremio_env()
    start = time.time()
    executor = DremioQueryExecutor()
    try:
        executor.execute_queries_with_configs(
            queries=[rec["sql"]],
            output_configs=[rec["output_config"]],
        )
        elapsed = round(time.time() - start, 2)
        update_query_log(query_name, rec["frequency"], "success", elapsed)
        log_query_performance(query_name, elapsed)
        print(f"✅ Query executed successfully: {query_name} in {elapsed} seconds")
    except Exception as exc:
        msg = str(exc)
        update_query_log(query_name, rec["frequency"], "failed", None, msg)
        print(f"❌ Error executing {query_name}: {msg}")


# ===================== #
# Run Ad-hoc query      #
# ===================== #


def run_single_query(query_name: str, sql_dir: str = "sql", force_run: bool = False) -> None:
    """
    Run a single query ad-hoc without running other scheduled queries.

    Args:
        query_name: Exact file name of the SQL file to run, e.g. 'account_summary.sql'.
        sql_dir: Directory where SQL files are stored (default: 'sql').
        force_run: If True, mark the query as a forced run in the log.
    """
    initialize_duckdb_query_log()
    initialize_duckdb_query_performance_log()

    registry = build_registry(sql_dir=sql_dir)

    if query_name not in registry:
        print(f"❌ Query not found: {query_name}")
        return

    record = registry[query_name]
    if force_run:
        record["force_run"] = True

    print(f"▶ Running single query: {query_name} (force_run={force_run})")
    execute_query(query_name, record)


# ============================== #
# Run Multiple Queries Ad-hoc    #
# ============================== #


def run_multiple_queries(*query_names: str, sql_dir: str = "sql", force_run: bool = False) -> None:
    """
    Run multiple queries ad-hoc without running other scheduled queries.

    Args:
        query_names: One or more SQL file names to run, e.g. 'file1.sql', 'file2.sql'.
        sql_dir: Directory where SQL files are stored (default: 'sql').
        force_run: If True, mark each query as a forced run in the log.
    """
    initialize_duckdb_query_log()
    initialize_duckdb_query_performance_log()
    registry = build_registry(sql_dir=sql_dir)

    for query_name in query_names:
        if query_name not in registry:
            print(f"❌ Query not found: {query_name}")
            continue

        record = registry[query_name]
        if force_run:
            record["force_run"] = True

        print(f"▶ Running query: {query_name} (force_run={force_run})")
        execute_query(query_name, record)


# ============================================= #
# Helper function to mark queries as success    #
# ============================================= #


def mark_queries_as_success(queries: Dict[str, Dict[str, str]]) -> None:
    """
    Insert a success record into DuckDB query_log for each query in the given dict.

    Args:
        queries: Dict of {query_name: {"frequency": str}}

    Example:

        quarterly_queries = {
            "account.sql": {"frequency": "quarterly"},
            "lead.sql": {"frequency": "quarterly"},
            "naics.sql": {"frequency": "quarterly"},
        }

        mark_queries_as_success(quarterly_queries)
    """
    for query_name, meta in queries.items():
        frequency = meta.get("frequency", DEFAULT_FREQUENCY)
        update_query_log(
            query_name=query_name,
            frequency=frequency,
            status="success",
            execution_time=None,
            error_message="Marked as Success",
        )
        print(f"✅ Marked {query_name} as success in log")


# override_queries = {
#     "account.sql": {"frequency": "quarterly"},
#     "lead.sql": {"frequency": "quarterly"},
#     "naics.sql": {"frequency": "weekly"},
# }

# mark_queries_as_success(override_queries)

# ================================================================ #
# Helper functions to list all queries or show X most recent runs  #
# There are more duckdb helper functions in #
# ================================================================ #


def list_queries(sql_dir: str = "sql") -> None:
    registry = build_registry(sql_dir=sql_dir)
    print("\nAvailable queries:")
    for name in sorted(registry.keys()):
        print(f" - {name}")


def show_log(limit: int = 34) -> None:
    with duckdb.connect(DUCKDB_FILE) as con:
        rows = con.execute("SELECT * FROM query_log ORDER BY last_run DESC LIMIT ?", (limit,)).fetchdf()
        print(rows)


# ===================================== #
# Main function to kick everything off  #
# ===================================== #


def main() -> None:
    initialize_duckdb_query_log()
    initialize_duckdb_query_performance_log()

    registry = build_registry(sql_dir="sql")

    to_run = {
        name: rec
        for name, rec in registry.items()
        if should_run_query(name, rec["frequency"], rec.get("force_run", False))
    }
    if not to_run:
        print("\n**All Scheduled Queries Have Run**\n")
        return
        # ================================================ #
        # Reduce max_workers if needed                     #
        # ================================================ #
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        futures = {pool.submit(execute_query, n, r): n for n, r in to_run.items()}
        for f in concurrent.futures.as_completed(futures):
            f.result()

    FILE_MANAGER.run()
    print("\n\n\n===============Finished Running All Queries===============\n\n\n")


# %%


# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
###############################################
# =========================================== #
# Execution Block - Run Above and Below Here  #
# =========================================== #
###############################################


main()  # comment this line out if running via cli


# ===================== #
# Execute SOQL Queries  #
# ===================== #

from platypus.salesforce_client import SalesforceQueryExecutor

import soql.soql_queries as soql_queries

soql_querys = [
    soql_queries.employee_soql,
    soql_queries.user_role_soql,
    soql_queries.user_soql,
    soql_queries.opportunity_soql,
    soql_queries.campaign_member_soql,
]

soql_output_configs = [
    {"output_format": "parquet", "output_filename": "employee"},
    {"output_format": "parquet", "output_filename": "user_role"},
    {"output_format": "parquet", "output_filename": "user"},
    {"output_format": "parquet", "output_filename": "opportunity"},
    {"output_format": "parquet", "output_filename": "campaign_member"},
]

SalesforceQueryExecutor().execute_queries_with_configs(soql_querys, soql_output_configs)


# %%
# =================================================== #
# Run Single or Multiple ad-hoc queries               #
# run_multiple_queries executes queries synchronously #
# =================================================== #
## run_single_query("bc_listings.sql", force_run=True)
## run_multiple_queries("record_type.sql", "account_relationship.sql", force_run=True)

# run_multiple_queries(
#     "record_type.sql",
#     "account_relationship.sql",
#     "account_summary.sql",
#     "contact.sql",
#     "customer_summary.sql",
#     "tms_package_analysis.sql",
#     "financial_account.sql",
#     "naics_code_hierarchy",
#     force_run=True,
# )
