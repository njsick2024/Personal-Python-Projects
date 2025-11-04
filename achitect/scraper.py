# %%
from __future__ import annotations

# ================================ CONFIG ================================
# TLS and network behavior

VERIFY_SSL = False  # True, False, or path to corporate CA bundle
FORCE_TLS12 = True  # pin TLS 1.2 b/c network is fussy
IGNORE_SYSTEM_PROXIES = True  # ignore HTTP(S)_PROXY env vars
SUPPRESS_INSECURE_WARNINGS = True  # hide noisy SSL warnings when VERIFY_SSL is False

# Input CSV of routing numbers
CSV_PATH = r"input\odfi_rdfi_numbers.csv"
CSV_COLUMN = None  # set None to auto detect first column
CSV_DELIMITER = ","
CSV_HAS_HEADER = True

# Politeness
REQUEST_TIMEOUT = 20
RETRY_TOTAL = 3
RETRY_BACKOFF_SEC = 1.5
SLEEP_BETWEEN_SEC = 0.6
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " "(KHTML, like Gecko) Chrome/125.0 Safari/537.36"
)

DETAIL_URL = "https://www.usbanklocations.com/routing-number-{rn}.html"

# DuckDB output
DUCKDB_FILE = "db/aba_lookup.duckdb"
ACH_TABLE = "aba_fedach"
WIRE_TABLE = "aba_fedwire"
FAIL_TABLE = "aba_failures"

# CSV exports
OUTPUT_DIR = "output"
EXPORT_ACH_CSV = True
EXPORT_WIRE_CSV = True
EXPORT_JOINED_CSV = True
EXPORT_FAILURES_CSV = True
# =======================================================================

import datetime as dt
import re
import ssl
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import duckdb
import pandas as pd
import requests
import urllib3
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ========================= Utilities and Setup ==========================


# Adapter that applies a custom ssl.SSLContext to urllib3 pools and proxies.
class SSLContextAdapter(HTTPAdapter):
    def __init__(self, ssl_context: ssl.SSLContext | None = None, **kwargs):
        self._ssl_context = ssl_context
        super().__init__(**kwargs)

    # Attach our SSL context when creating connection pools
    def init_poolmanager(self, *args, **pool_kwargs):
        if self._ssl_context is not None:
            pool_kwargs["ssl_context"] = self._ssl_context
        return super().init_poolmanager(*args, **pool_kwargs)

    # Also attach it when using proxies
    def proxy_manager_for(self, proxy, **proxy_kwargs):
        if self._ssl_context is not None:
            proxy_kwargs["ssl_context"] = self._ssl_context
        return super().proxy_manager_for(proxy, **proxy_kwargs)


# Ensure db and output directories exist.
def ensure_dirs() -> None:
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    Path(DUCKDB_FILE).parent.mkdir(parents=True, exist_ok=True)


# ========================== CSV Loading ============================


# Read routing numbers from CSV, preserve leading zeros, validate to nine digits.
def load_aba_numbers_from_csv(
    path: str,
    column: Optional[str] = None,
    delimiter: str = ",",
    has_header: bool = True,
) -> List[str]:
    if has_header:
        df = pd.read_csv(path, dtype=str, sep=delimiter)
        if column is None:
            candidates = list(df.columns)
            prefer = ["routing_number", "routing", "aba", "aba_number", "rtn"]
            pick = None
            for p in prefer:
                for c in candidates:
                    if c.strip().lower() == p:
                        pick = c
                        break
                if pick:
                    break
            if not pick:
                pick = candidates[0]
            column = pick
        ser = df[column].astype(str)
    else:
        df = pd.read_csv(path, dtype=str, sep=delimiter, header=None, names=["aba"])
        ser = df["aba"].astype(str)

    cleaned = ser.fillna("").str.replace(r"\D", "", regex=True).str.zfill(9)
    valid = cleaned[cleaned.str.fullmatch(r"\d{9}")]
    unique = pd.unique(valid)
    return [str(x) for x in unique if isinstance(x, str)]


# ========================== HTTP Session ===========================


# Build a hardened Requests session with retries and a custom SSL context.
def build_session() -> requests.Session:
    ctx = ssl.create_default_context()
    if FORCE_TLS12:
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.maximum_version = ssl.TLSVersion.TLSv1_2
    if not VERIFY_SSL:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

    s = requests.Session()
    if IGNORE_SYSTEM_PROXIES:
        s.trust_env = False

    retries = Retry(
        total=RETRY_TOTAL,
        backoff_factor=RETRY_BACKOFF_SEC,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )

    adapter = SSLContextAdapter(ssl_context=ctx, max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)

    s.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Connection": "close",  # keeps sockets short lived on touchy TLS paths
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )

    if SUPPRESS_INSECURE_WARNINGS and not VERIFY_SSL:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    return s


# ============================ Fetch Page ===========================


# Fetch one detail page and return html, status code, and url for logging.
def fetch_detail_html(session: requests.Session, rn: str) -> Tuple[Optional[str], int, str]:
    url = DETAIL_URL.format(rn=rn.strip())
    status = 0
    try:
        resp = session.get(url, timeout=REQUEST_TIMEOUT, verify=VERIFY_SSL)
        status = resp.status_code
        if resp.status_code != 200:
            return None, status, url
        return resp.text, status, url
    except Exception:
        return None, status, url


# ============================ Parse HTML ===========================


# Parse a label value section that follows an H2 like FedACH Routing.
def parse_section(h2_tag: Tag) -> Dict[str, str]:
    data: Dict[str, str] = {}
    current_key: Optional[str] = None

    for sib in h2_tag.next_siblings:
        if isinstance(sib, Tag) and sib.name in {"h2", "h1"}:
            break
        tokens = list(sib.stripped_strings) if isinstance(sib, Tag) else []
        for tok in tokens:
            if ":" in tok:
                key, val = tok.split(":", 1)
                key = key.strip()
                val = val.strip()
                current_key = key
                data[key] = val if val else data.get(key, "")
            else:
                if current_key:
                    prev = data.get(current_key, "")
                    data[current_key] = (prev + " " + tok).strip()

    for k in list(data.keys()):
        data[k] = re.sub(r"\s+", " ", data[k]).strip()
    return data


# Extract both sections and return raw dicts for normalization.
def parse_detail_page(html: str) -> Tuple[Dict[str, str], Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")  # lxml is faster if installed, html.parser is fine
    ach_h2 = soup.find("h2", string=re.compile(r"FedACH Routing", re.I))
    wire_h2 = soup.find("h2", string=re.compile(r"Fedwire Routing", re.I))
    ach = parse_section(ach_h2) if ach_h2 else {}
    wire = parse_section(wire_h2) if wire_h2 else {}
    return ach, wire


# =========================== Normalization =========================


# Pull out servicing fed routing and address from a mixed text field.
def parse_servicing(text: str) -> Tuple[Optional[str], Optional[str]]:
    if not text:
        return None, None
    m = re.match(r"^\s*(\d{9})\s*,\s*(.+)$", text)
    if m:
        return m.group(1), m.group(2).strip()
    m2 = re.search(r"(\d{9})", text)
    return (m2.group(1) if m2 else None, text.strip() or None)


# Convert common date formats to ISO date strings for DuckDB DATE ingestion.
def to_date(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d"):
        try:
            return dt.datetime.strptime(s.strip(), fmt).date().isoformat()
        except Exception:
            pass
    return None


# Normalize a FedACH dict to a single row.
def normalize_ach_row(rn: str, d: Dict[str, str]) -> Dict[str, object]:
    fed_rtn, fed_addr = parse_servicing(d.get("Servicing Fed's Main Office", ""))
    return {
        "routing_number": rn,
        "bank_name": d.get("Name"),
        "address_full": d.get("Address"),
        "phone": d.get("Phone"),
        "office_type": d.get("Type"),
        "servicing_fed_main_office_rtn": fed_rtn,
        "servicing_fed_main_office_addr": fed_addr,
        "status": d.get("Status"),
        "change_date": to_date(d.get("Change Date")),
        "scraped_at": dt.datetime.utcnow().isoformat(sep=" ", timespec="seconds"),
        "source_url": DETAIL_URL.format(rn=rn),
    }


# Normalize a Fedwire dict to a single row.
def normalize_wire_row(rn: str, d: Dict[str, str]) -> Dict[str, object]:
    return {
        "routing_number": rn,
        "bank_name": d.get("Name"),
        "telegraphic_name": d.get("Telegraphic Name"),
        "location": d.get("Location"),
        "funds_transfer_status": d.get("Funds Transfer Status"),
        "book_entry_securities_transfer_status": d.get("Book-Entry Securities Transfer Status"),
        "revision_date": to_date(d.get("Revision Date")),
        "scraped_at": dt.datetime.utcnow().isoformat(sep=" ", timespec="seconds"),
        "source_url": DETAIL_URL.format(rn=rn),
    }


# ============================== DuckDB =============================


# Create tables on first run. Failures table records warn or fail with details.
def init_duckdb(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {ACH_TABLE} (
            routing_number TEXT PRIMARY KEY,
            bank_name TEXT,
            address_full TEXT,
            phone TEXT,
            office_type TEXT,
            servicing_fed_main_office_rtn TEXT,
            servicing_fed_main_office_addr TEXT,
            status TEXT,
            change_date DATE,
            scraped_at TIMESTAMP,
            source_url TEXT
        );
        """
    )
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {WIRE_TABLE} (
            routing_number TEXT PRIMARY KEY,
            bank_name TEXT,
            telegraphic_name TEXT,
            location TEXT,
            funds_transfer_status TEXT,
            book_entry_securities_transfer_status TEXT,
            revision_date DATE,
            scraped_at TIMESTAMP,
            source_url TEXT
        );
        """
    )
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {FAIL_TABLE} (
            id BIGINT,
            routing_number TEXT,
            url TEXT,
            http_status INTEGER,
            level TEXT,
            error_type TEXT,
            error_message TEXT,
            when_utc TIMESTAMP
        );
        """
    )


# Upsert a frame into a DuckDB table on routing_number primary key.
def merge_into_duckdb(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, table: str) -> None:
    if df.empty:
        return
    con.register("staging_df", df)
    cols = ",".join(df.columns)
    sets = ",".join([f"{c}=EXCLUDED.{c}" for c in df.columns if c != "routing_number"])
    con.execute(
        f"""
        INSERT INTO {table} ({cols})
        SELECT {cols} FROM staging_df
        ON CONFLICT (routing_number) DO UPDATE SET {sets};
        """
    )
    con.unregister("staging_df")


# Upsert a frame into a DuckDB table on routing_number primary key
def merge_into_duckdb(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, table: str) -> None:
    if df.empty:
        return
    con.register("staging_df", df)
    cols = ",".join(df.columns)
    sets = ",".join([f"{c}=EXCLUDED.{c}" for c in df.columns if c != "routing_number"])
    con.execute(
        f"""
        INSERT INTO {table} ({cols})
        SELECT {cols} FROM staging_df
        ON CONFLICT (routing_number) DO UPDATE SET {sets};
        """
    )
    con.unregister("staging_df")


# Append failures or warnings into the failures table.
def append_failures(con: duckdb.DuckDBPyConnection, failures: pd.DataFrame) -> None:
    if failures.empty:
        return
    con.register("fail_df", failures)
    con.execute(
        f"""
        INSERT INTO {FAIL_TABLE} (
            routing_number, url, http_status, level, error_type, error_message, when_utc
        )
        SELECT routing_number, url, http_status, level, error_type, error_message, when_utc
        FROM fail_df;
        """
    )
    con.unregister("fail_df")


# ============================== Exports ============================


# Write the three success exports, plus optional failures CSV with timestamp.
def export_csvs(ach_df: pd.DataFrame, wire_df: pd.DataFrame, failures_df: pd.DataFrame) -> None:
    ensure_dirs()

    if EXPORT_ACH_CSV and not ach_df.empty:
        ach_path = Path(OUTPUT_DIR) / "aba_fedach.csv"
        ach_df.to_csv(ach_path, index=False, encoding="utf-8")
        print(f"[CSV] wrote {ach_path}")

    if EXPORT_WIRE_CSV and not wire_df.empty:
        wire_path = Path(OUTPUT_DIR) / "aba_fedwire.csv"
        wire_df.to_csv(wire_path, index=False, encoding="utf-8")
        print(f"[CSV] wrote {wire_path}")

    if EXPORT_JOINED_CSV:
        joined = pd.merge(ach_df, wire_df, on="routing_number", how="outer", suffixes=("_ach", "_wire"))
        if "bank_name_ach" in joined.columns and "bank_name_wire" in joined.columns:
            joined["bank_name"] = joined["bank_name_ach"].combine_first(joined["bank_name_wire"])
            joined.drop(columns=[c for c in ["bank_name_ach", "bank_name_wire"] if c in joined.columns], inplace=True)

        key_cols = [
            "routing_number",
            "bank_name",
            "address_full",
            "phone",
            "office_type",
            "telegraphic_name",
            "location",
            "funds_transfer_status",
            "book_entry_securities_transfer_status",
            "servicing_fed_main_office_rtn",
            "servicing_fed_main_office_addr",
            "status",
            "change_date",
            "revision_date",
            "source_url_ach",
            "source_url_wire",
            "scraped_at_ach",
            "scraped_at_wire",
        ]
        cols = [c for c in key_cols if c in joined.columns] + [c for c in joined.columns if c not in key_cols]
        joined = joined[cols]

        joined_path = Path(OUTPUT_DIR) / "aba_routing_directory.csv"
        joined.to_csv(joined_path, index=False, encoding="utf-8")
        print(f"[CSV] wrote {joined_path}")

    if EXPORT_FAILURES_CSV and not failures_df.empty:
        ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        fail_path = Path(OUTPUT_DIR) / f"aba_failures_{ts}.csv"
        failures_df.to_csv(fail_path, index=False, encoding="utf-8")
        print(f"[CSV] wrote {fail_path}")


# ============================== Main ===============================
# Orchestrate the run: load CSV, fetch pages, parse, normalize, persist, export.


def main() -> None:
    ensure_dirs()

    # Load input
    aba_numbers = load_aba_numbers_from_csv(
        CSV_PATH, column=CSV_COLUMN, delimiter=CSV_DELIMITER, has_header=CSV_HAS_HEADER
    )
    if not aba_numbers:
        print("[WARN] No valid routing numbers loaded from CSV")
        return

    # Scrape
    session = build_session()
    ach_rows: list[Dict[str, object]] = []
    wire_rows: list[Dict[str, object]] = []
    fail_rows: list[Dict[str, object]] = []

    for idx, rn in enumerate(aba_numbers, start=1):
        html, status, url = fetch_detail_html(session, rn)

        # Handle non 200 or request error
        if html is None:
            fail_rows.append(
                {
                    "routing_number": rn,
                    "url": url,
                    "http_status": status or None,
                    "level": "FAIL",
                    "error_type": "HTTPError" if status else "RequestException",
                    "error_message": f"status={status}" if status else "request failed",
                    "when_utc": dt.datetime.utcnow().isoformat(sep=" ", timespec="seconds"),
                }
            )
            print(f"[FAIL] {rn} http_status={status} url={url}")
            time.sleep(SLEEP_BETWEEN_SEC)
            continue

        # Parse and validate sections
        try:
            ach_raw, wire_raw = parse_detail_page(html)
            if not ach_raw and not wire_raw:
                fail_rows.append(
                    {
                        "routing_number": rn,
                        "url": url,
                        "http_status": status or None,
                        "level": "WARN",
                        "error_type": "MissingContent",
                        "error_message": "no FedACH or Fedwire section found",
                        "when_utc": dt.datetime.utcnow().isoformat(sep=" ", timespec="seconds"),
                    }
                )
                print(f"[WARN] {rn} missing content")
                time.sleep(SLEEP_BETWEEN_SEC)
                continue

            ach_rows.append(normalize_ach_row(rn, ach_raw))
            wire_rows.append(normalize_wire_row(rn, wire_raw))
            print(f"[OK] {rn} parsed ({idx}/{len(aba_numbers)})")

        except Exception as e:
            fail_rows.append(
                {
                    "routing_number": rn,
                    "url": url,
                    "http_status": status or None,
                    "level": "FAIL",
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "when_utc": dt.datetime.utcnow().isoformat(sep=" ", timespec="seconds"),
                }
            )
            print(f"[FAIL] {rn} {type(e).__name__}: {e}")

        time.sleep(SLEEP_BETWEEN_SEC)

    # Persist
    ach_df = pd.DataFrame(ach_rows)
    wire_df = pd.DataFrame(wire_rows)
    failures_df = pd.DataFrame(
        fail_rows, columns=["routing_number", "url", "http_status", "level", "error_type", "error_message", "when_utc"]
    )

    con = duckdb.connect(DUCKDB_FILE)
    init_duckdb(con)
    merge_into_duckdb(con, ach_df, ACH_TABLE)
    merge_into_duckdb(con, wire_df, WIRE_TABLE)
    append_failures(con, failures_df)
    con.close()

    # Exports
    export_csvs(ach_df, wire_df, failures_df)

    # Summary
    print(f"[DONE] wrote {len(ach_df)} ACH rows, {len(wire_df)} Wire rows into {DUCKDB_FILE}")
    if not failures_df.empty:
        warn_ct = (failures_df["level"] == "WARN").sum() if "level" in failures_df.columns else 0
        fail_ct = (failures_df["level"] == "FAIL").sum() if "level" in failures_df.columns else 0
        print(f"[DONE] recorded {warn_ct} warnings and {fail_ct} failures in {FAIL_TABLE}")


# %%

main()
