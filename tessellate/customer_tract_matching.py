# %%
from pathlib import Path

import duckdb

# ---------- CONFIG ----------
# Customer Parquet with columns: customer_id, latitude, longitude
PARQUET_PATH = Path(r"customer_summary.parquet")

# Folder containing tl_2024_XX_tract.zip files (one or many)
TIGER_DIR = Path(r"zips")

OUTPUT_DIR = Path("output")
DB_PATH = "customers.duckdb"

PARQUET_OUT = OUTPUT_DIR / "customers_with_tract.parquet"
CSV_OUT = OUTPUT_DIR / "customers_with_tract.csv"
# -----------------------------------


# Load every tract shapefile from all zip files in a folder.
# Read each zip with GDAL vsizip and append into a single tracts table
def load_tracts_from_zips(con: duckdb.DuckDBPyConnection, tiger_dir: Path) -> None:
    zip_files = sorted(tiger_dir.glob("*.zip"))
    if not zip_files:
        raise RuntimeError(f"No .zip files found in {tiger_dir.as_posix()} for TIGER tracts")

    first = True
    for z in zip_files:
        zip_path = z.as_posix()
        inner_shp = z.with_suffix("").name + ".shp"

        con.execute(
            """
            CREATE OR REPLACE TABLE _tracts_chunk AS
            SELECT * FROM ST_Read('/vsizip/' || ? || '/' || ?)
            """,
            [zip_path, inner_shp],
        )

        if first:
            con.execute("CREATE OR REPLACE TABLE tracts AS SELECT * FROM _tracts_chunk")
            first = False
        else:
            con.execute("INSERT INTO tracts SELECT * FROM _tracts_chunk")

    con.execute("DROP TABLE IF EXISTS _tracts_chunk")


def prepare_tracts(con: duckdb.DuckDBPyConnection) -> None:
    """
    Create a normalized, reprojected view of tracts for joining.
    TIGER tracts are NAD83 (EPSG:4269). Reproject to WGS84 (EPSG:4326).
    """
    con.execute(
        """
        CREATE OR REPLACE VIEW tracts_4326 AS
        SELECT
              STATEFP::VARCHAR AS state_fips
            , COUNTYFP::VARCHAR AS county_fips
            , TRACTCE::VARCHAR AS ce_tract
            , GEOID::VARCHAR   AS geo_id
            , ST_Transform(geom, 'EPSG:4269', 'EPSG:4326') AS geom_4326
        FROM tracts;
        """
    )


def register_customers_from_parquet(con: duckdb.DuckDBPyConnection, parquet_path: Path) -> None:
    """
    Create customers table and geometry view.
    Uses lon for longitude everywhere.
    """
    con.execute(
        """
        CREATE OR REPLACE TABLE customers AS
        SELECT
              CAST(customer_id AS VARCHAR) AS customer_id
            , CAST(latitude  AS DOUBLE) AS lat
            , CAST(longitude AS DOUBLE) AS lon
        FROM read_parquet(?)
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL;
        """,
        [str(parquet_path)],
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW customers_geom AS
        SELECT
              customer_id, lat, lon,
              ST_Point(lon, lat) AS geom_4326
        FROM customers;
        """
    )


def join_customers_to_tracts(con: duckdb.DuckDBPyConnection) -> None:
    """Point-in-polygon join of customers to tracts."""
    con.execute(
        """
        CREATE OR REPLACE TABLE customers_with_tract AS
        SELECT
              c.customer_id
            , c.lat
            , c.lon
            , t.state_fips
            , t.county_fips
            , t.ce_tract
            , t.geo_id
            , c.geom_4326 AS customer_geom
        FROM customers_geom c
        LEFT JOIN tracts_4326 t
          ON ST_Within(c.geom_4326, t.geom_4326);
        """
    )


def write_outputs(con: duckdb.DuckDBPyConnection) -> None:
    """Write Parquet with geometry and CSV without geometry."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # IMPORTANT: DuckDB does not support parameter binding in COPY paths.
    # Inline the path string instead of using "TO ?".
    # con.execute(f"COPY customers_with_tract TO '{PARQUET_OUT.as_posix()}' (FORMAT PARQUET)")

    con.execute(
        f"""
        COPY (
            SELECT
                customer_id
                , lat
                , lon
                , geo_id
                , state_fips
                , county_fips
                , ce_tract
            FROM customers_with_tract
        ) TO '{PARQUET_OUT.as_posix()}' (FORMAT PARQUET);
    """
    )

    # CSV (drops geometry); you can add ST_AsText(customer_geom) if you want WKT
    con.execute(
        f"""
        COPY (
            SELECT
                  customer_id
                , lat
                , lon
                , geo_id
                , state_fips
                , county_fips
                , ce_tract
            FROM customers_with_tract
            ORDER BY customer_id
        ) TO '{CSV_OUT.as_posix()}' WITH (HEADER, DELIM ',');
        """
    )


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=DB_PATH)
    try:
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")

        register_customers_from_parquet(con, PARQUET_PATH)
        load_tracts_from_zips(con, TIGER_DIR)
        prepare_tracts(con)

        # Join + outputs
        join_customers_to_tracts(con)
        write_outputs(con)

        n_customers = con.execute("SELECT COUNT(*) FROM customers;").fetchone()[0]
        n_joined = con.execute("SELECT COUNT(*) FROM customers_with_tract;").fetchone()[0]

        print(f"Customers read: {n_customers:,}")
        print(f"Customers joined to tracts: {n_joined:,}")
        print("Wrote:")
        print(f"  {PARQUET_OUT}")
        print(f"  {CSV_OUT}")

    finally:
        con.close()


if __name__ == "__main__":
    main()
# %%# %%
