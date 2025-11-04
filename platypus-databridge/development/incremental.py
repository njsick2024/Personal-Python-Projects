
from __future__ import annotations

import concurrent.futures
import contextlib
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Iterator, List, Optional, Protocol, Sequence, Tuple

import duckdb
import polars as pl
from dotenv import load_dotenv


# =========================
# Decorators and utilities
# =========================

def timed(fn: Callable) -> Callable:
    """
    Measures wall time of a function and prints a short line on completion.
    """

    def wrapper(*args, **kwargs):
        start = time.time()
        try:
            return fn(*args, **kwargs)
        finally:
            dur = time.time() - start
            print(f"[timed] {fn.__name__} finished in {dur:.2f}s")

    return wrapper


def retry(
    attempts: int = 3,
    delay_seconds: float = 1.0,
    backoff: float = 2.0,
    retry_on: Tuple[type, ...] = (Exception,),
) -> Callable:
    """
    Simple retry for flaky IO paths like network or Arrow Flight reads.
    """

    def deco(fn: Callable) -> Callable:
        def wrapped(*args, **kwargs):
            tries = 0
            cur_delay = delay_seconds
            while True:
                tries += 1
                try:
                    return fn(*args, **kwargs)
                except retry_on as e:
                    if tries >= attempts:
                        print(f"[retry] {fn.__name__} failed after {tries} attempts: {e}")
                        raise
                    print(f"[retry] {fn.__name__} attempt {tries} failed: {e}. Retrying in {cur_delay:.1f}s")
                    time.sleep(cur_delay)
                    cur_delay *= backoff

        return wrapped

    return deco


# =====================================
# Interfaces and core value objects
# =====================================

class DremioLike(Protocol):
    """
    Minimal interface needed from the dremio_simple_query client.
    """

    def toArrow(self, sql: str):
        ...


@dataclass(frozen=True)
class TableSpec:
    """
    Describes a Dremio to DuckDB sync target.
    """
    dremio_path: str
    local_name: str
    primary_key: str = "id"
    last_modified_column: str = "last_modified_ts"


# =====================================
# DuckDB connection manager and repos
# =====================================

class DuckDBSession:
    """
    Context manager for a DuckDB connection with sensible pragmas.
    Ensures the database is on disk, not in memory.
    """

    def __init__(
        self,
        db_path: Path,
        memory_limit_fraction: float = 0.80,
        temp_directory: Optional[Path] = None,
        threads: Optional[int] = None,
    ):
        self.db_path = Path(db_path)
        self.memory_limit_fraction = memory_limit_fraction
        self.temp_directory = temp_directory
        self.threads = threads
        self.con: Optional[duckdb.DuckDBPyConnection] = None

    def __enter__(self) -> duckdb.DuckDBPyConnection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(self.db_path.as_posix())
        # Pragmas
        # memory_limit supports values like 4GB or 80 percent of system memory via percentage string
        # We estimate memory as a fraction and pass as percent string for portability
        pct = int(self.memory_limit_fraction * 100)
        con.execute(f"PRAGMA memory_limit='{pct}%';")
        if self.temp_directory:
            self.temp_directory.mkdir(parents=True, exist_ok=True)
            con.execute(f"PRAGMA temp_directory='{self.temp_directory.as_posix()}';")
        if self.threads and self.threads > 0:
            con.execute(f"PRAGMA threads={int(self.threads)};")
        # Safety pragmas
        con.execute("PRAGMA enable_progress_bar=false;")
        con.execute("PRAGMA preserve_insertion_order=false;")
        self.con = con
        return con

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.con is not None:
            self.con.close()
        self.con = None


class SyncMetadataRepository:
    """
    Reads and writes last sync timestamps per table.
    """

    def __init.dir_safe_create(self, con: duckdb.DuckDBPyConnection) -> None:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS sync_metadata (
                table_name VARCHAR PRIMARY KEY,
                last_sync_ts BIGINT
            );
            """
        )

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self.con = con
        self.__init.dir_safe_create(con)

    def get_last_sync_ts(self, table_name: str) -> int:
        row = self.con.execute(
            "SELECT last_sync_ts FROM sync_metadata WHERE table_name = ?;",
            [table_name],
        ).fetchone()
        return int(row[0]) if row else 0

    def set_last_sync_ts(self, table_name: str, ts_ms: int) -> None:
        self.con.execute(
            "INSERT OR REPLACE INTO sync_metadata VALUES (?, ?);",
            [table_name, int(ts_ms)],
        )


# =====================================
# Sync service
# =====================================

class IncrementalSyncService:
    """
    Service that performs incremental merges from Dremio into DuckDB.
    """

    def __init__(self, dremio: DremioLike, db_path: Path):
        self.dremio = dremio
        self.db_path = Path(db_path)

    @staticmethod
    def _make_incremental_sql(spec: TableSpec, last_sync_ms: int) -> str:
        return (
            f"SELECT * FROM {spec.dremio_path} "
            f"WHERE {spec.last_modified_column} > {last_sync_ms}"
        )

    @staticmethod
    def _ensure_target_exists(con: duckdb.DuckDBPyConnection, local_name: str, staging_view: str) -> None:
        con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {local_name} AS
            SELECT * FROM {staging_view} WHERE 1 = 0
            """
        )

    @staticmethod
    def _merge_sql(local_name: str, pk: str, all_columns: Sequence[str], staging_view: str) -> str:
        set_list = ", ".join([f"target.{c} = source.{c}" for c in all_columns])
        return f"""
        MERGE INTO {local_name} AS target
        USING {staging_view} AS source
        ON target.{pk} = source.{pk}
        WHEN MATCHED THEN UPDATE SET {set_list}
        WHEN NOT MATCHED THEN INSERT *
        """

    @retry(attempts=3, delay_seconds=1.0, backoff=2.0)
    def _fetch_incremental_arrow_reader(self, sql: str):
        return self.dremio.toArrow(sql).to_reader()

    @timed
    def sync_one(
        self,
        spec: TableSpec,
        memory_limit_fraction: float = 0.80,
        temp_dir: Optional[Path] = None,
        threads: Optional[int] = None,
    ) -> Tuple[str, int]:
        """
        Performs one table sync and returns the number of merged rows.
        """
        print(f"\n[start] Sync for {spec.local_name}")
        with DuckDBSession(self.db_path, memory_limit_fraction, temp_dir, threads) as con:
            meta = SyncMetadataRepository(con)
            last_ms = meta.get_last_sync_ts(spec.local_name)
            cur_ms = int(time.time() * 1000)

            sql = self._make_incremental_sql(spec, last_ms)
            print(f"[info] Incremental predicate is {spec.last_modified_column} > {last_ms}")

            # Pull incremental data
            reader = self._fetch_incremental_arrow_reader(sql)
            schema_names = list(reader.schema.names)
            if not schema_names:
                print("[warn] No schema returned, skip")
                meta.set_last_sync_ts(spec.local_name, cur_ms)
                return spec.local_name, 0

            # Zero copy stream into Polars
            staged_pl = pl.from_arrow(reader)
            n = staged_pl.height
            if n == 0:
                print("[info] No new rows")
                meta.set_last_sync_ts(spec.local_name, cur_ms)
                return spec.local_name, 0

            print(f"[info] Fetched {n} rows from Dremio")

            # Register as a view for fast MERGE
            con.register("staging_data_arrow", staged_pl.to_arrow())
            con.execute("CREATE OR REPLACE VIEW staging_view AS SELECT * FROM staging_data_arrow")

            # Ensure target table exists
            self._ensure_target_exists(con, spec.local_name, "staging_view")

            # Validate primary key exists
            if spec.primary_key not in staged_pl.columns:
                raise ValueError(
                    f"Primary key column {spec.primary_key} not present in incoming data for {spec.local_name}"
                )

            # Merge
            merge_sql = self._merge_sql(
                local_name=spec.local_name,
                pk=spec.primary_key,
                all_columns=staged_pl.columns,
                staging_view="staging_view",
            )
            con.execute(merge_sql)

            # Update metadata
            meta.set_last_sync_ts(spec.local_name, cur_ms)

            # Clean up view registration
            con.unregister("staging_data_arrow")

            print(f"[done] Merged {n} rows into {spec.local_name}")
            return spec.local_name, int(n)

    @timed
    def sync_many(
        self,
        specs: Sequence[TableSpec],
        parallel: bool = False,
        workers: Optional[int] = None,
        memory_limit_fraction: float = 0.80,
        temp_dir: Optional[Path] = None,
        threads: Optional[int] = None,
    ) -> List[Tuple[str, int]]:
        """
        Sync many tables sequentially or in parallel.
        """
        results: List[Tuple[str, int]] = []

        if not parallel:
            for spec in specs:
                results.append(self.sync_one(spec, memory_limit_fraction, temp_dir, threads))
            return results

        max_workers = workers or min(8, os.cpu_count() or 4)
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as pool:
            futures = [
                pool.submit(
                    _sync_entrypoint_subprocess,
                    self.db_path.as_posix(),
                    spec,
                    memory_limit_fraction,
                    temp_dir.as_posix() if temp_dir else None,
                    threads,
                    os.environ.get("TOKEN"),
                    os.environ.get("ARROW_ENDPOINT"),
                )
                for spec in specs
            ]
            for fut in concurrent.futures.as_completed(futures):
                results.append(fut.result())
        return results


# =====================================
# Subprocess entry point for parallel
# =====================================

def _sync_entrypoint_subprocess(
    db_path: str,
    spec: TableSpec,
    memory_limit_fraction: float,
    temp_dir: Optional[str],
    threads: Optional[int],
    token: Optional[str],
    endpoint: Optional[str],
) -> Tuple[str, int]:
    """
    Executed in a separate process to avoid Arrow or DuckDB resource contention.
    """
    from dremio_simple_query.connect import DremioConnection  # local import to isolate dependency

    if not token or not endpoint:
        raise RuntimeError("Missing TOKEN or ARROW_ENDPOINT for subprocess")

    dremio = DremioConnection(token, endpoint)
    service = IncrementalSyncService(dremio=dremio, db_path=Path(db_path))
    return service.sync_one(
        spec,
        memory_limit_fraction=memory_limit_fraction,
        temp_dir=Path(temp_dir) if temp_dir else None,
        threads=threads,
    )


# =====================================
# Optional test harness with fake Dremio
# =====================================

class _FakeFlightStreamReader:
    def __init__(self, tbl: pl.DataFrame):
        import pyarrow as pa

        self._reader = tbl.to_arrow().to_reader()

    @property
    def schema(self):
        return self._reader.schema

    def __getattr__(self, item):
        return getattr(self._reader, item)


class _FakeFlightStream:
    def __init__(self, tbl: pl.DataFrame):
        self.tbl = tbl

    def to_reader(self):
        return _FakeFlightStreamReader(self.tbl)


class FakeDremioClient:
    """
    Minimal fake with toArrow that returns an Arrow compatible object.
    """

    def __init__(self, source_df: pl.DataFrame):
        self.source = source_df

    def toArrow(self, sql: str):
        # Very basic predicate handling for tests based on last_modified_ts
        # This is only for the demo harness
        cutoff = 0
        if "WHERE" in sql:
            try:
                cutoff = int(sql.split("WHERE")[1].split(">")[1].strip())
            except Exception:
                cutoff = 0
        filtered = self.source.filter(pl.col("last_modified_ts") > cutoff)
        return _FakeFlightStream(filtered)


# =====================================
# Main script entry
# =====================================

def _load_real_dremio_or_exit() -> Optional[DremioLike]:
    """
    Loads .env and returns a live DremioConnection if possible, else None.
    """
    load_dotenv()
    token = os.getenv("TOKEN")
    endpoint = os.getenv("ARROW_ENDPOINT")
    if not token or not endpoint:
        print("ENV missing TOKEN or ARROW_ENDPOINT. Running fake demo instead.")
        return None
    try:
        from dremio_simple_query.connect import DremioConnection
    except Exception as e:
        print(f"dremio_simple_query not importable: {e}. Running fake demo instead.")
        return None
    try:
        return DremioConnection(token, endpoint)
    except Exception as e:
        print(f"Could not connect to Dremio: {e}. Running fake demo instead.")
        return None


def demo_fake_run(db_path: Path) -> None:
    """
    Runs a small end to end sync using the FakeDremioClient and a temporary DuckDB file.
    This acts as a smoke test.
    """
    print("\n[demo] starting fake run")
    df = pl.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["alpha", "bravo", "charlie"],
            "last_modified_ts": [1_000, 2_000, 3_000],
        }
    )
    fake = FakeDremioClient(df)
    service = IncrementalSyncService(fake, db_path)
    spec = TableSpec(dremio_path="any.path", local_name="demo_table", primary_key="id", last_modified_column="last_modified_ts")
    # first sync gets all three
    service.sync_one(spec)
    # second sync gets none
    service.sync_one(spec)
    # update two rows and one new row
    df2 = pl.DataFrame(
        {
            "id": [2, 3, 4],
            "name": ["bravo2", "charlie2", "delta"],
            "last_modified_ts": [4_000, 5_000, 6_000],
        }
    )
    fake.source = df2
    service.sync_one(spec)

    # Inspect results
    with DuckDBSession(db_path) as con:
        print("\n[demo] final contents")
        print(con.execute("SELECT * FROM demo_table ORDER BY id").fetchall())
        print(con.execute("SELECT * FROM sync_metadata").fetchall())


def main() -> None:
    """
    If real Dremio creds exist in environment, performs a real sync.
    Otherwise runs the fake demo.
    """
    db_path = Path("local_warehouse.duckdb")
    dremio = _load_real_dremio_or_exit()

    if dremio is None:
        demo_fake_run(db_path)
        return

    # Real run based on your initial list
    service = IncrementalSyncService(dremio, db_path)
    specs = [
        TableSpec("dremio_source.sales.customers", "customers_local"),
        TableSpec("dremio_source.hr.employees", "employees_local"),
        TableSpec("dremio_source.inventory.products", "products_local"),
        # Add more specs here
    ]

    print(f"DuckDB database file: {db_path.as_posix()}")
    results = service.sync_many(
        specs,
        parallel=False,            # flip to True to enable multi process sync
        workers=None,              # set a number to cap processes
        memory_limit_fraction=0.80,
        temp_dir=Path("duck_temp"),
        threads=None,              # let DuckDB choose
    )

    print("\nSummary")
    for name, n in results:
        print(f"{name}: {n} merged")


if __name__ == "__main__":
    main()


