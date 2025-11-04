```
 █████╗  ██████╗██╗  ██╗██╗████████╗███████╗ ██████╗████████╗
██╔══██╗██╔════╝██║  ██║██║╚══██╔══╝██╔════╝██╔════╝╚══██╔══╝
███████║██║     ███████║██║   ██║   █████╗  ██║        ██║   
██╔══██║██║     ██╔══██║██║   ██║   ██╔══╝  ██║        ██║   
██║  ██║╚██████╗██║  ██║██║   ██║   ███████╗╚██████╗   ██║   
╚═╝  ╚═╝ ╚═════╝╚═╝  ╚═╝╚═╝   ╚═╝   ╚══════╝ ╚═════╝   ╚═╝ 
```

## 📚 Table of Contents
1. [Overview](#overview)
2. [Project Structure](#project-structure)
3. [Configuration](#configuration)
4. [Usage](#usage)
5. [Outputs and Exports](#outputs-and-exports)
6. [Data Model](#data-model)
9. [FAQ](#faq)


---

## 🧾 Overview

ACHitect reads a CSV of routing numbers and fetches each detail

It parses the **FedACH Routing** and **Fedwire Routing** sections, normalizes fields like dates and Servicing Fed Main Office, then

* Upserts into DuckDB tables `aba_fedach` and `aba_fedwire`
* Exports three CSVs in the `output` folder, including a joined directory for easy lookup

Everything is functional Python with configs at the top of `scraper.py`.

---

Tips

* Keep `VERIFY_SSL = False` only while you sort out the corporate certificate chain  
  When ready, set `VERIFY_SSL = True` or point to your CA bundle path
* If you need a proxy, set `IGNORE_SYSTEM_PROXIES = False` and configure `session.proxies` after building the session

---

## 🚀 Usage

1) Place your input CSV where you want and set `CSV_PATH` and `CSV_COLUMN`.

2) Run the script

3) Watch the console for progress. Each routing number prints an `[OK]` line after parsing.

Results land in DuckDB and the `output` folder.

---

## 📄 Outputs and Exports

* `db/aba_lookup.duckdb`  
  * `aba_fedach` table with FedACH fields  
  * `aba_fedwire` table with Fedwire fields

* `output/aba_fedach.csv`  
* `output/aba_fedwire.csv`  
* `output/aba_routing_directory.csv`  
  An outer join on `routing_number` that coalesces `bank_name` with preference for the ACH value and orders common fields for readability

Toggle exports with `EXPORT_ACH_CSV`, `EXPORT_WIRE_CSV`, and `EXPORT_JOINED_CSV`.

---


## 🧱 Data Model

The script creates tables if they do not exist and writes idempotently keyed by `routing_number`.

```sql
-- FedACH
CREATE TABLE IF NOT EXISTS aba_fedach (
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

-- Fedwire
CREATE TABLE IF NOT EXISTS aba_fedwire (
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
```

Write strategy

* Insert with conflict update on `routing_number` so repeated runs refresh fields without duplicates

---

## 🔍 DuckDB Tips

Open the file in a DuckDB shell or through Python.

```sql
-- from DuckDB shell
.open db/aba_lookup.duckdb

-- sanity checks
SELECT COUNT(*) FROM aba_fedach;
SELECT COUNT(*) FROM aba_fedwire;

-- find ACH present but Wire absent
SELECT a.routing_number, a.bank_name
FROM aba_fedach a
LEFT JOIN aba_fedwire w USING (routing_number)
WHERE w.routing_number IS NULL
ORDER BY 1
LIMIT 50;

-- look up a single routing number
SELECT * FROM aba_fedach WHERE routing_number = '273970116';
```

---

## 🛡️ Troubleshooting TLS and Proxies

Use these knobs while you confirm the trust chain, then return to strict verification.

* Handshake or EOF errors  
  * Set `FORCE_TLS12 = True`
  * Keep the `Connection: close` header which is already set

* Corporate proxy with TLS inspection  
  * Point `VERIFY_SSL` to your corporate CA file path
  * Or set `REQUESTS_CA_BUNDLE` to that path before running

* Proxies  
  * If you need the proxy, leave `IGNORE_SYSTEM_PROXIES = False`
  * You can also set `session.proxies` explicitly after `build_session()`

Security reminder

* Turn verification back on once the certificate chain is trusted on your machine

---

## ❓ FAQ

**What CSV headers are required**  
None. If `CSV_COLUMN` is `None`, the loader uses the first column and treats values as text, preserving leading zeros.

**What happens if a page is missing or does not have the expected sections**  
That routing number is skipped. The console prints a warning.

**Can I export only the joined directory**  
Yes. Set `EXPORT_ACH_CSV = False` and `EXPORT_WIRE_CSV = False` while keeping `EXPORT_JOINED_CSV = True`.

**Can I run this from a scheduler**  
Yes. The script has no global state outside DuckDB and the output folder, so it is safe to run on a schedule.

---

## 🤝 Notes on Responsible Use

* Respect the target site terms and robots guidance
* Keep polite defaults such as a short sleep between requests and modest retry counts
* Cache locally and avoid repeated fetches for the same routing numbers

---

