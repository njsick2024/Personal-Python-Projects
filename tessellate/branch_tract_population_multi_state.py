# %%
from pathlib import Path
from typing import Iterable, Tuple

import duckdb

# ================================ CONFIG ================================
DB_PATH = "multi_state.duckdb"
OUTPUT_DIR = Path(r"output")

# Branches CSV must have columns: branch_id, lat, lon. Generate this via dremio - see sql folder: branch_coords
BRANCH_COORD_CSV = Path(r"input\branch_coords\branch_coords.csv")

# Folder containing TIGER tract zip files
TIGER_DIR = Path(r"zips")

# FFIEC style census CSV with no header
CENSUS_CSV = Path(r"input\census\census_2025.csv")

# Optional exports and debug options
WRITE_EXPORTS = True
SHOW_SAMPLE_AFTER = True
SAMPLE_TABLE = "branch_tract_membership_with_pop_urban"
# =========================================================================


# Ensure output directory exists and create the output directory tree if needed
def ensure_output_dir(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)


# Create a DuckDB connection and load spatial extension
def init_duckdb(db_path: str) -> duckdb.DuckDBPyConnection:

    con = duckdb.connect(db_path)
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")
    return con


# Drop any leftover tables or views from a previous run
def clean_slate(con: duckdb.DuckDBPyConnection) -> None:
    to_drop = [
        "tracts",
        "tract_point_on_surface_deg",
        "branches",
        "branches_clean",
        "census_raw",
        "tract_pop_geoid",
        "radii_miles",
        "tracts_within_branch_radius",
        "branch_tract_counts",
        "branch_pop_tract",
        "branch_pop_tract_wide",
        "tracts_within_branch_radius_detail",
        "_tracts_chunk",
        "branch_tract_membership",
        "branch_tract_membership_with_pop",
        "branch_tract_membership_with_pop_urban",
    ]
    for name in to_drop:
        con.execute(f"DROP TABLE IF EXISTS {name}")
        con.execute(f"DROP VIEW  IF EXISTS {name}")


# Load every tract shapefile from all zip files in a folder. Read each zip with GDAL vsizip and append into a single tracts table
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


# Build tract point on surface
# Make valid polygons, use point on surface, and store lat lon for quick distance checks
# ST_PointOnSurface guarantees your point is within the polygon
# Whereas a centroid based uses a center of mass calculation which causes points to lie outside to tract
# Centroid based also breaks spatial containment logic when this occurs


def build_tract_point_on_surface_deg(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE OR REPLACE TABLE tract_point_on_surface_deg AS
        SELECT
              GEOID
            , ST_Y(ST_PointOnSurface(ST_MakeValid(geom))) AS lat
            , ST_X(ST_PointOnSurface(ST_MakeValid(geom))) AS lon
            , STATEFP
            , COUNTYFP
            , TRACTCE
            , ALAND
            , AWATER
        FROM tracts
        WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom)
        """
    )


# Read branches from CSV and keep only valid lat lon pairs
def load_and_clean_branches(con: duckdb.DuckDBPyConnection, branches_csv: Path) -> None:
    con.execute(
        """
        CREATE OR REPLACE TABLE branches AS
        SELECT
              CAST(branch_id AS VARCHAR) AS branch_id
            , CAST(lat AS DOUBLE) AS lat
            , CAST(lon AS DOUBLE) AS lon
        FROM read_csv_auto(?, header=true)
        """,
        [branches_csv.as_posix()],
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE branches_clean AS
        SELECT
              CAST(branch_id AS VARCHAR) AS branch_id
            , lon
            , lat
        FROM branches
        WHERE lon BETWEEN -180 AND 180
          AND lat BETWEEN -90  AND 90
          AND lon IS NOT NULL
          AND lat IS NOT NULL
        """
    )


# Load FFIEC census file and build a tract geoid table with indicators
# Read as varchar to preserve zero padded codes then normalize to geoid and numeric types
def load_and_build_census(con: duckdb.DuckDBPyConnection, census_csv: Path) -> None:
    con.execute(
        """
        CREATE OR REPLACE TABLE census_raw AS
        SELECT * FROM read_csv(?, header=false, delim=',', all_varchar=true)
        """,
        [census_csv.as_posix()],
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE tract_pop_geoid AS
        WITH norm AS (
          SELECT
                lpad(trim(column0002), 2, '0') AS state_fip
              , lpad(trim(column0003), 3, '0') AS county_fip
              , lpad(replace(trim(column0004), '.', ''), 6, '0') AS tract
              , trim(column0005) AS principal_city
              , trim(column0006) AS small_county
              , trim(column0007) AS split_tract
              , trim(column0008) AS demographics_ind
              , trim(column0009) AS urban_rural_ind
              , trim(column0010) AS median_fam_income_msamd
              , trim(column0011) AS median_hh_income_msamd
              , trim(column0012) AS median_fam_income_prcnt_msamd
              , trim(column0013) AS median_fam_income_ffiec
              , trim(column0014) AS cra_income_ind
              , trim(column0015) AS cra_poverty
              , trim(column0016) AS cra_unemployment
              , trim(column0017) AS cra_distressed
              , trim(column0018) AS cra_remote_area
              , nullif(replace(trim(column0022), ',', ''), '') AS pop_txt
          FROM census_raw
        )
        SELECT
                state_fip || county_fip || tract AS geoid
              , state_fip
              , county_fip
              , tract
              , try_cast(pop_txt AS BIGINT) AS population
              , cra_income_ind
              , CASE WHEN cra_income_ind = '1' THEN 'low'
                   WHEN cra_income_ind = '2' THEN 'moderate'
                   WHEN cra_income_ind = '3' THEN 'middle'
                   WHEN cra_income_ind = '4' THEN 'upper'
                   WHEN cra_income_ind = '0' THEN 'NA'
                   ELSE NULL END AS cra_income_class
              , CASE WHEN cra_income_ind IN ('1','2') THEN 1 ELSE 0 END AS lmi_ind
              , CASE WHEN cra_poverty = 'X' THEN 1 ELSE 0 END AS cra_poverty
              , CASE WHEN cra_unemployment = 'X' THEN 1 ELSE 0 END AS cra_unemployment
              , CASE WHEN cra_distressed = 'X' THEN 1 ELSE 0 END AS cra_distressed
              , CASE WHEN cra_remote_area = 'X' THEN 1 ELSE 0 END AS cra_remote_area
              , principal_city
              , small_county
              , split_tract
              , demographics_ind
              , urban_rural_ind
              , try_cast(median_fam_income_msamd AS DECIMAL(18,2)) AS median_fam_income_msamd
              , try_cast(median_hh_income_msamd AS DECIMAL(18,2)) AS median_hh_income_msamd
              , try_cast(median_fam_income_prcnt_msamd AS DECIMAL(18,2)) AS median_fam_income_prcnt_msamd
              , try_cast(median_fam_income_ffiec AS DECIMAL(18,2)) AS median_fam_income_ffiec
        FROM norm
        WHERE pop_txt IS NOT NULL
        """
    )


# Create a Haversine macro that returns meters
# Define a reusable macro for distance on the sphere with mean Earth radius in meters
# You could also use ST_Distance_Sphere. I chose to create a macro for fun, proof of concept, and for example usage.
def create_haversine_macro(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE OR REPLACE MACRO haversine_m(lat1, lon1, lat2, lon2) AS (
          2*6371000*asin(
            sqrt(
              sin((radians(lat2 - lat1))/2)*sin((radians(lat2 - lat1))/2) +
              cos(radians(lat1))*cos(radians(lat2))*sin((radians(lon2 - lon1))/2)*sin((radians(lon2 - lon1))/2)
            )
          )
        )
        """
    )


# Create a small table of search radii in miles and meters
# These values are standard mile to meter conversions... from google
def create_radii(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE OR REPLACE TABLE radii_miles AS
        SELECT * FROM (
          VALUES (3, CAST(4828.032 AS DOUBLE)),
                 (5, CAST(8046.72 AS DOUBLE)),
                 (10,CAST(16093.44 AS DOUBLE))
        ) AS t(miles, meters)
        """
    )


# Build the set of tracts that fall within 3/5/10 miles of each branch
# Uses haversine distance between the branch coordinates and each tracts (ST_PointOnSurface).
# If the tracts point lies within the branch radius, the tract is included.
def build_tracts_within_radius(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE OR REPLACE TABLE tracts_within_branch_radius AS
        SELECT DISTINCT
              CAST(b.branch_id AS VARCHAR) AS branch_id
            , r.miles
            , t.GEOID
        FROM branches_clean b
        CROSS JOIN radii_miles r
        JOIN tract_point_on_surface_deg t
          ON haversine_m(b.lat, b.lon, t.lat, t.lon) <= r.meters
        """
    )


# Aggregate counts of tracts in radius by branch and miles
# Count how many tracts are within each distance bucket for each branch
def aggregate_counts(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE OR REPLACE TABLE branch_tract_counts AS
        SELECT
            branch_id
          , miles
          , COUNT(*) AS tracts_in_radius
        FROM tracts_within_branch_radius
        GROUP BY 1,2
        ORDER BY 1,2
        """
    )


# Aggregate population sums for those tracts by branch and miles
# Sum population from the normalized census table for the selected tract sets
def aggregate_population(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE OR REPLACE TABLE branch_pop_tract AS
        SELECT
              CAST(c.branch_id AS VARCHAR) AS branch_id
            , c.miles
            , SUM(p.population) AS population_in_radius
        FROM tracts_within_branch_radius c
        JOIN tract_pop_geoid p
          ON p.geoid = c.GEOID
        GROUP BY 1,2
        ORDER BY 1,2
        """
    )


# Create wide output with one row per branch and columns for the three mile buckets
# Pivot the population sums into three columns for quick consumption
def build_wide_outputs(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE OR REPLACE TABLE branch_pop_tract_wide AS
        SELECT
              CAST(branch_id AS VARCHAR) AS branch_id
            , SUM(CASE WHEN miles=3 THEN population_in_radius ELSE 0 END) AS pop_3mi
            , SUM(CASE WHEN miles=5 THEN population_in_radius ELSE 0 END) AS pop_5mi
            , SUM(CASE WHEN miles=10 THEN population_in_radius ELSE 0 END) AS pop_10mi
        FROM branch_pop_tract
        GROUP BY 1
        ORDER BY 1
        """
    )


# Create a detail table with tract metadata and population for each branch and miles
# Include GEOID, tract code, state code, county code and population
def build_detail_output(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE OR REPLACE TABLE tracts_within_branch_radius_detail AS
        SELECT DISTINCT
                CAST(i.branch_id AS VARCHAR) AS branch_id
              , i.miles
              , i.GEOID AS geoid
              , t.TRACTCE AS ce_tract
              , t.STATEFP AS state_fips
              , t.COUNTYFP AS county_fips
              , COALESCE(p.population, 0) AS population
        FROM tracts_within_branch_radius AS i
        JOIN tracts AS t
          ON t.GEOID = i.GEOID
        LEFT JOIN tract_pop_geoid AS p
          ON p.geoid = i.GEOID
        ORDER BY 1,2,3
        """
    )


# Export branch census tract mapping with 3, 5, 10 mile population
# Transform the point from EPSG 4326 to 4269 to match TIGER tract geometry
# Returns true if the branch point lies strictly within the tract polygon
def build_branch_tract_membership(con: duckdb.DuckDBPyConnection) -> None:
    # Base mapping of branch to its containing tract
    con.execute(
        """
        CREATE OR REPLACE TABLE branch_tract_membership AS
        SELECT
              CAST(b.branch_id AS VARCHAR) AS branch_id
            , t.GEOID AS geoid
            , t.TRACTCE AS ce_tract
        FROM branches_clean b
        JOIN tracts t
          ON ST_Within(
               ST_Transform(ST_Point(b.lon, b.lat), 'EPSG:4326', 'EPSG:4269'),
               ST_MakeValid(t.geom)
             )
        """
    )

    # Add population at 3, 5, 10 miles from the wide table built above.
    con.execute(
        """
        CREATE OR REPLACE TABLE branch_tract_membership_with_pop AS
        SELECT
              m.branch_id
            , m.geoid
            , m.ce_tract
            , w.pop_3mi
            , w.pop_5mi
            , w.pop_10mi
        FROM branch_tract_membership m
        LEFT JOIN branch_pop_tract_wide w
          ON w.branch_id = m.branch_id
        ORDER BY 1
        """
    )


# Export branch census tract membership with 3, 5, 10 mile population w/ urban rural indicator
# Uses polygon containment (ST_Within)
def build_branch_tract_membership_with_urban_rural(con: duckdb.DuckDBPyConnection) -> None:
    # Base mapping of branch to its containing tract
    con.execute(
        """
        CREATE OR REPLACE TABLE branch_tract_membership AS
        SELECT
              CAST(b.branch_id AS VARCHAR) AS branch_id
            , t.GEOID AS geoid
            , t.TRACTCE AS ce_tract
        FROM branches_clean b
        JOIN tracts t
          ON ST_Within(
               ST_Transform(ST_Point(b.lon, b.lat), 'EPSG:4326', 'EPSG:4269'),
               ST_MakeValid(t.geom)
             )
        """
    )

    # Add population at 3, 5, 10 miles from the wide table, plus urban_rural_ind and ALAND/AWATER.
    # Also compute is_water = 1 if AWATER >= 50% of (ALAND + AWATER), else 0.
    con.execute(
        """
        CREATE OR REPLACE TABLE branch_tract_membership_with_pop_urban AS
        WITH ranked AS (
        SELECT
              CAST(m.branch_id AS VARCHAR) AS branch_id
            , m.geoid
            , m.ce_tract
            , w.pop_3mi 
            , w.pop_5mi 
            , w.pop_10mi
            , p.urban_rural_ind
            , tpos.ALAND
            , tpos.AWATER
            , CASE 
                WHEN (COALESCE(tpos.ALAND, 0) + COALESCE(tpos.AWATER, 0)) > 0
                     AND COALESCE(tpos.AWATER, 0) * 1.0 
                         / (COALESCE(tpos.ALAND, 0) + COALESCE(tpos.AWATER, 0)) >= 0.5
                  THEN 1 ELSE 0
              END AS is_water
            , ROW_NUMBER() OVER (
                PARTITION BY m.branch_id
                ORDER BY m.geoid NULLS LAST, m.ce_tract NULLS LAST
            ) AS rn
        FROM branch_tract_membership m
        LEFT JOIN branch_pop_tract_wide w
            ON w.branch_id = m.branch_id
        LEFT JOIN tract_pop_geoid p
            ON p.geoid = m.geoid
        LEFT JOIN tract_point_on_surface_deg tpos
            ON tpos.GEOID = m.geoid
        )
        SELECT
          branch_id
        , geoid
        , ce_tract
        , pop_3mi
        , pop_5mi
        , pop_10mi
        , urban_rural_ind
        , ALAND AS area_land
        , AWATER AS area_water
        , is_water
        FROM ranked
        WHERE rn = 1
        ORDER BY branch_id;
        """
    )


# Print sanity checks
def print_sanity(con: duckdb.DuckDBPyConnection) -> None:
    tables = [
        "tracts",
        "tract_point_on_surface_deg",
        "branches_clean",
        "tract_pop_geoid",
        "tracts_within_branch_radius",
        "branch_tract_counts",
        "branch_pop_tract",
        "branch_pop_tract_wide",
        "tracts_within_branch_radius_detail",
        "branch_tract_membership_with_pop",
        "branch_tract_membership_with_pop_urban",
    ]
    for name in tables:
        n = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        print(f"{name}: {n} rows")


# Export a table to parquet and csv with optional sort order
# Write parquet with COPY and produce a csv for easy sharing
def export_table(con: duckdb.DuckDBPyConnection, out_dir: Path, name: str, sort_cols: str = "1,2") -> Tuple[Path, Path]:
    pq_path = out_dir / f"{name}.parquet"
    cs_path = out_dir / f"{name}.csv"

    # con.execute("COPY " + name + " TO ? (FORMAT PARQUET)", [pq_path.as_posix()])
    con.execute(f"COPY {name} TO '{pq_path.as_posix()}' (FORMAT PARQUET)")

    df = con.execute(f"SELECT * FROM {name} ORDER BY {sort_cols}").fetch_df()
    df.to_csv(cs_path, index=False)
    print(f"Wrote:\n  {pq_path}\n  {cs_path}")
    return pq_path, cs_path


# Optional utility to show a small sample after the run
# Print a small DataFrame preview for the chosen table
def show_sample(con: duckdb.DuckDBPyConnection, table: str, limit: int = 10) -> None:
    print("\nSample")
    print(con.execute(f"SELECT * FROM {table} ORDER BY 1 LIMIT {limit}").df())


# Drive everything from end to end
# Prepare file system and database then run each step in order
def main() -> None:
    ensure_output_dir(OUTPUT_DIR)
    con = init_duckdb(DB_PATH)

    clean_slate(con)
    load_tracts_from_zips(con, TIGER_DIR)
    build_tract_point_on_surface_deg(con)

    load_and_clean_branches(con, BRANCH_COORD_CSV)
    load_and_build_census(con, CENSUS_CSV)

    create_haversine_macro(con)
    create_radii(con)

    build_tracts_within_radius(con)
    aggregate_counts(con)
    aggregate_population(con)

    build_wide_outputs(con)
    build_detail_output(con)
    build_branch_tract_membership(con)
    build_branch_tract_membership_with_urban_rural(con)

    print_sanity(con)

    if WRITE_EXPORTS:
        export_table(con, OUTPUT_DIR, "branch_pop_tract_wide", "1")
        export_table(con, OUTPUT_DIR, "branch_pop_tract", "1,2")
        export_table(con, OUTPUT_DIR, "branch_tract_counts", "1,2")
        export_table(con, OUTPUT_DIR, "tracts_within_branch_radius_detail", "1,2")
        export_table(con, OUTPUT_DIR, "branch_tract_membership_with_pop", "1")
        export_table(con, OUTPUT_DIR, "branch_tract_membership_with_pop_urban", "1")

    if SHOW_SAMPLE_AFTER:
        show_sample(con, SAMPLE_TABLE, limit=10)

    print("Done")


# %%
main()


# %%
