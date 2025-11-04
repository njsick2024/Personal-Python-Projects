# %%
from pathlib import Path

import duckdb

# ---------- ------------------------------CONFIGS --------------------------------------------------
DB_PATH = "texas.duckdb"
OUTPUT_DIR = Path(r"output\sales_enablement_team")
BRANCH_COORD_CSV = Path(r"input\tx_bc_coords\tx_bc_coords.csv")  # From Dremio branch_sumamry
TIGER_ZIP = Path(r"input\tx_geo_files\tl_2024_48_tract.zip")
CENSUS_CSV = Path(r"input\census\census_2025.csv")
# ---------------------------------------------------------------------------------------------------

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(DB_PATH)
con.execute("INSTALL spatial;")
con.execute("LOAD spatial;")

# Clean slate: drop prior objects
to_drop = [
    "tracts",
    "tract_centroids_tx_deg",
    "branches",
    "branches_clean",
    "census_raw",
    "tract_pop_geoid",
    "radii_miles",
    "tx_branch_tracts_in_radius_centroid",
    "tx_branch_tract_counts_centroid",
    "tx_branch_pop_centroid",
    "tx_branch_pop_centroid_wide",
]
for name in to_drop:
    con.execute(f"DROP TABLE IF EXISTS {name};")
    con.execute(f"DROP VIEW  IF EXISTS {name};")


# Load Texas tracts from TIGER ZIP
zip_path = TIGER_ZIP.as_posix()
inner_shp = "tl_2024_48_tract.shp"
con.execute(
    f"""
    CREATE OR REPLACE TABLE tracts AS
    SELECT * FROM ST_Read('/vsizip/{zip_path}/{inner_shp}');
"""
)


# Tract centroids in geographic degrees
con.execute(
    """
    CREATE OR REPLACE TABLE tract_centroids_tx_deg AS
    SELECT
        GEOID
    , ST_Y(ST_PointOnSurface(ST_MakeValid(geom))) AS lat
    , ST_X(ST_PointOnSurface(ST_MakeValid(geom))) AS lon
    FROM tracts
    WHERE STATEFP='48'
    AND geom IS NOT NULL
    AND NOT ST_IsEmpty(geom);
"""
)

# Branches: read & clean (degrees)
con.execute(
    f"""
    CREATE OR REPLACE TABLE branches AS
    SELECT
    CAST(branch_id AS VARCHAR) AS branch_id
    , CAST(lat AS DOUBLE) AS lat
    , CAST(lon AS DOUBLE) AS lon
    FROM read_csv_auto('{BRANCH_COORD_CSV.as_posix()}', header=true);
"""
)

con.execute(
    """
    CREATE OR REPLACE TABLE branches_clean AS
    SELECT 
    branch_id
    , lon
    , lat
    FROM branches
    WHERE lon BETWEEN -180 AND 180
    AND lat BETWEEN -90  AND 90
    AND lon IS NOT NULL
    AND lat IS NOT NULL;
"""
)

# Read raw as VARCHAR to preserve leading zeros and allow padding
con.execute(
    f"""
CREATE OR REPLACE TABLE census_raw AS
SELECT * FROM read_csv('{CENSUS_CSV.as_posix()}', header=false, delim=',', all_varchar=true);
"""
)

# Build 11-digit GEOID and grab fields
con.execute(
    """
CREATE OR REPLACE TABLE tract_pop_geoid AS
WITH norm AS (
  SELECT
      lpad(trim(column0002), 2, '0') AS state_fip           -- index 3
    , lpad(trim(column0003), 3, '0') AS county_fip          -- index 4
    , lpad(replace(trim(column0004), '.', ''), 6, '0')  AS tract            -- index 5 (implied decimal)
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
    , nullif(replace(trim(column0022), ',', ''), '') AS pop_txt             -- index 23 (Total persons)
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
           ELSE NULL
      END                AS cra_income_class
    , CASE WHEN cra_income_ind IN ('1','2') THEN 1 ELSE 0 END AS lmi_ind
    , CASE WHEN cra_poverty = 'X' THEN 1 ELSE 0 END  AS cra_poverty
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

# Haversine distance (meters)
con.execute(
    """
CREATE OR REPLACE MACRO haversine_m(lat1, lon1, lat2, lon2) AS (
  2*6371000*asin(
    sqrt(
      sin((radians(lat2 - lat1))/2)*sin((radians(lat2 - lat1))/2) +
      cos(radians(lat1))*cos(radians(lat2))*sin((radians(lon2 - lon1))/2)*sin((radians(lon2 - lon1))/2)
    )
  )
);
"""
)

# Radii (miles -> meters)
con.execute(
    """
    CREATE OR REPLACE TABLE radii_miles AS
    SELECT * FROM (
    VALUES (3, CAST(4828.032 AS DOUBLE)),
           (5, CAST(8046.72 AS DOUBLE)),
           (10,CAST(16093.44 AS DOUBLE))
    ) AS t(miles, meters);
"""
)

# Distinct tract sets by centroid-Haversine
con.execute(
    """
    CREATE OR REPLACE TABLE tx_branch_tracts_in_radius_centroid AS
    SELECT DISTINCT
      b.branch_id
    , r.miles
    , t.GEOID
    FROM branches_clean b
    CROSS JOIN radii_miles r
    JOIN tract_centroids_tx_deg t
    ON haversine_m(b.lat, b.lon, t.lat, t.lon) <= r.meters;
"""
)

# Counts & Population: counts per branch + radius
con.execute(
    """
    CREATE OR REPLACE TABLE tx_branch_tract_counts_centroid AS
    SELECT 
      branch_id
    , miles
    , COUNT(*) AS tracts_in_radius
    FROM tx_branch_tracts_in_radius_centroid
    GROUP BY 1,2
    ORDER BY 1,2;
"""
)

# Population sums per branch + radius
con.execute(
    """
    CREATE OR REPLACE TABLE tx_branch_pop_centroid AS
    SELECT
      c.branch_id
    , c.miles
    , SUM(p.population) AS population_in_radius
    FROM tx_branch_tracts_in_radius_centroid c
    JOIN tract_pop_geoid p
    ON p.geoid = c.GEOID
    GROUP BY 1,2
    ORDER BY 1,2;
"""
)

# Per-branch (3/5/10 mile columns)
con.execute(
    """
    CREATE OR REPLACE TABLE tx_branch_pop_centroid_wide AS
    SELECT
        branch_id
    , SUM(CASE WHEN miles=3 THEN population_in_radius ELSE 0 END) AS pop_3mi
    , SUM(CASE WHEN miles=5 THEN population_in_radius ELSE 0 END) AS pop_5mi
    , SUM(CASE WHEN miles=10 THEN population_in_radius ELSE 0 END) AS pop_10mi
    FROM tx_branch_pop_centroid
    GROUP BY 1
    ORDER BY 1;
"""
)

# Add population for each tract per branch & radius (centroid method)
con.execute(
    """
    CREATE OR REPLACE TABLE tx_branch_tracts_in_radius_centroid_detail AS
    SELECT DISTINCT
          i.branch_id
        , i.miles
        , i.GEOID as geoid
        , t.TRACTCE as ce_tract
        , COALESCE(p.population, 0) AS population
    FROM tx_branch_tracts_in_radius_centroid AS i
    JOIN tracts AS t
    ON t.GEOID = i.GEOID
    AND t.STATEFP = '48'
    LEFT JOIN tract_pop_geoid AS p
    ON p.geoid = i.GEOID
    ORDER BY 1,2,3;
    """
)


# Sanity Checks
for name in [
    "tracts",
    "tract_centroids_tx_deg",
    "branches_clean",
    "tract_pop_geoid",
    "tx_branch_tracts_in_radius_centroid",
    "tx_branch_tract_counts_centroid",
    "tx_branch_pop_centroid",
    "tx_branch_pop_centroid_wide",
]:
    n = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
    print(f"{name}: {n} rows")

print("\nSample population:")
print(con.execute("SELECT * FROM tx_branch_pop_centroid_wide ORDER BY branch_id LIMIT 10").df())


# Exports
def export_table(name: str, sort_cols="1,2"):
    pq = OUTPUT_DIR / f"{name}.parquet"
    cs = OUTPUT_DIR / f"{name}.csv"
    con.execute(f"COPY {name} TO '{pq.as_posix()}' (FORMAT PARQUET)")
    df = con.execute(f"SELECT * FROM {name} ORDER BY {sort_cols}").fetch_df()
    df.to_csv(cs, index=False)
    print(f"Wrote:\n  {pq}\n  {cs}")


for t in [
    "tx_branch_pop_centroid_wide",
    "tract_pop_geoid",
    "tx_branch_tracts_in_radius_centroid_detail",
    # "tx_branch_tract_counts_centroid",
    # "tx_branch_pop_centroid",
]:
    export_table(t, sort_cols="1,2" if "wide" not in t else "1")

print("Done.")

# %%
# -----------------------------------------------------------------
# ----------------------Helper Function ---------------------------
# -----------------------------------------------------------------

# -----------------------------------------------------------------
# Instatiate the connection with duckdb if needed
# -----------------------------------------------------------------

import duckdb

conn = duckdb.connect("texas.duckdb")


# -----------------------------------------------------------------
# Show all tables
# -----------------------------------------------------------------
tables_df = conn.execute("PRAGMA show_tables;").df()
print(tables_df)

# -----------------------------------------------------------------
# Show all columns for a specific table
# -----------------------------------------------------------------
TABLE = "tx_branch_pop_centroid"  # <-- change to your table name
cols_df = conn.execute(f"PRAGMA table_info('{TABLE}');").df()
print(cols_df)

# -----------------------------------------------------------------
# Print all tables with all their columns
# -----------------------------------------------------------------
all_cols_df = conn.execute(
    """
SELECT
    table_schema
  , table_name
  , ordinal_position AS column_ordinal
  , column_name
  , data_type
  , is_nullable
FROM information_schema.columns
WHERE table_schema NOT IN ('information_schema')
ORDER BY table_schema, table_name, ordinal_position;
"""
).df()

print(all_cols_df)

# -----------------------------------------------------------------
# Describe a table (schema summary)
# -----------------------------------------------------------------
desc_df = conn.execute(f"DESCRIBE {TABLE};").df()
print(desc_df)

# -----------------------------------------------------------------
# Print specific table
# -----------------------------------------------------------------
print(con.execute("select * from tx_branch_pop_centroid").df())
print(con.execute("select * from tx_branch_pop_centroid").df())
