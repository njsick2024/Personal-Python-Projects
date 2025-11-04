# %%
from pathlib import Path

import duckdb
import pandas as pd
import requests

# ---------- CONFIGS ----------
BRANCH_COORD_CSV = Path(r"input\tx_bc_coords\tx_bc_coords.csv")  # needs columns: branch_id, lat, long
OUTPUT_DIR = Path("output")
TIGER_ZIP = Path(r"input\tx_geo_files\tl_2024_48_tract.zip")  # Texas FIPS 48
# -----------------------------------


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database="texas.duckdb")

    # Install and load DuckDB spatial extension
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")

    # Read branches CSV into an in memory table
    con.execute(
        """
        CREATE OR REPLACE TABLE branches AS
        SELECT
            CAST(branch_id AS VARCHAR) AS branch_id,
            CAST(lat AS DOUBLE) AS lat,
            CAST(long AS DOUBLE) AS long
        FROM read_csv_auto(?, header=True)
    """,
        [str(BRANCH_COORD_CSV)],
    )

    # Build point geometry in EPSG 4326 for branches
    # ?:  Do we need to buildpoint geometry in EPSG 4326 for branches first then convert?

    con.execute(
        """
        CREATE OR REPLACE TABLE branches_geom AS
        SELECT
            branch_id, lat, long,
            ST_Point(long, lat) AS geom_4326
        FROM branches;
      """
    )

    # Read Texas tracts directly from the zip via GDAL virtual file system
    # Note: ST_Read returns a geometry column named geom and attributes such as GEOID
    # ?:  Do we need to buildpoint geometry in EPSG 4326 for branches first then convert?

    con.execute(
        """
        CREATE OR REPLACE TABLE tracts_raw AS
        SELECT *
        FROM ST_Read('/vsizip/{zip_path}/tl_2024_48_tract.shp')
    """.format(
            zip_path=str(TIGER_ZIP).replace("\\", "/")
        )
    )

    con.execute("CREATE OR REPLACE TABLE tracts AS SELECT * FROM tracts_raw;")

    con.execute(
        """
        CREATE OR REPLACE TABLE branch_match AS
        SELECT
            branch_id, lat, long,
            ST_Transform(geom_4326, 'EPSG:4326', 'EPSG:4269') AS geom
        FROM branches_geom;
    """
    )

    # Spatial join: which tract contains each branch point
    con.execute(
        """
        CREATE OR REPLACE TABLE tx_branches_with_tract AS
        SELECT
              c.branch_id
            , c.lat
            , c.long
            , t.STATEFP as state_fips 
            , t.COUNTYFP as county_fips
            , t.TRACTCE as ce_tract
            , t.GEOID as geo_id
            , c.geom AS branches_geom
        FROM branch_match c
        JOIN tracts t
          ON ST_Within(c.geom, t.geom)
    """
    )

    # Write outputs
    parquet_path = OUTPUT_DIR / "tx_branches_with_tract.parquet"
    csv_path = OUTPUT_DIR / "tx_branches_with_tract.csv"

    #! Parquet includes the binary geometry column
    con.execute(f"COPY tx_branches_with_tract TO '{parquet_path.as_posix()}' (FORMAT PARQUET)")

    # For CSV, drop binary geometry
    df = con.execute(
        """
        SELECT branch_id, lat, long, geo_id, state_fips, county_fips, ce_tract, ST_AsText(branches_geom) AS branches_geom_wkt
        FROM tx_branches_with_tract
        ORDER BY branch_id
    """
    ).fetch_df()
    df.to_csv(csv_path, index=False)

    print("Wrote:")
    print(f"  {parquet_path}")
    print(f"  {csv_path}")


main()
# %%
# if __name__ == "__main__":
#     main()


# AUTO_DOWNLOAD = True
# ensure_tiger_zip(TIGER_ZIP, TIGER_URL)

# TIGER_URL = "https://www2.census.gov/geo/tiger/TIGER2024/TRACT/tl_2024_48_tract.zip"

# def ensure_tiger_zip(zip_path: Path, url: str) -> None:
#     zip_path.parent.mkdir(parents=True, exist_ok=True)
#     if zip_path.exists():
#         return
#     if not AUTO_DOWNLOAD:
#         raise FileNotFoundError(f"{zip_path} not found and AUTO_DOWNLOAD is False")
#     print(f"Downloading {url} ...")
#     r = requests.get(url, timeout=180)
#     r.raise_for_status()
#     zip_path.write_bytes(r.content)
#     print(f"Wrote {zip_path}")#     print(f"Wrote {zip_path}")
#     print(f"Wrote {zip_path}")#     print(f"Wrote {zip_path}")
#     print(f"Wrote {zip_path}")#     print(f"Wrote {zip_path}")
