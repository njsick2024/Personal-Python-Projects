# %%
from pathlib import Path

import duckdb

CENSUS_CSV = Path("input/census/census_2025.csv")  # change to your path
con = duckdb.connect("texas.duckdb")
con.execute("INSTALL spatial;")
con.execute("LOAD spatial;")

# Read the FFIEC CSV with no header. DuckDB will names the columns.
con.execute(
    f"""
    CREATE OR REPLACE TABLE census_raw AS
    SELECT * FROM read_csv_auto('{CENSUS_CSV.as_posix()}', header=false, all_varchar=true)
"""
)


# Normalize fields using the column indexes from your data dictionary
con.execute(
    """
    CREATE OR REPLACE TABLE tract_pop_geoid AS
    WITH norm AS (
      SELECT
          lpad(trim(column0002), 2, '0') AS state_fip   -- index 3
        , lpad(trim(column0003), 3, '0') AS county_fip  -- index 4
        , lpad(replace(trim(column0004), '.', ''), 6, '0') AS tract -- index 5 (implied decimal)
        , trim(column0005) AS principal_city
        , trim(column0006) AS small_county
        , trim(column0007) AS split_tract
        , trim(column0008) AS demographics_ind
        , trim(column0009) AS urban_rural_ind
        , trim(column0010) AS median_fam_income_msamd
        , trim(column0011) AS median_hh_income_msamd
        , trim(column0012) AS median_fam_income_prcnt_msamd
        , trim(column0013) AS median_fam_income_ffiec
        , trim(column0014) AS cra_income_ind                          -- index 15 (Total persons)
        , trim(column0015) AS cra_poverty
        , trim(column0016) AS cra_unemployment
        , trim(column0017) AS cra_distressed
        , trim(column0018) AS cra_remote_area
        , nullif(replace(trim(column0022), ',', ''), '') AS pop_txt   -- index 23 (Total persons)
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
             END AS cra_income_class
      , CASE WHEN cra_income_ind = '1' OR cra_income_ind = '2' 
             THEN 1 ELSE 0 END AS lmi_ind 
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

OUTPUT_DIR = Path("output")
parquet_path = OUTPUT_DIR / "census_data.parquet"
con.execute(f"COPY tract_pop_geoid TO '{parquet_path.as_posix()}' (FORMAT PARQUET)")
