      $$$$$$$$\                                      $$\ $$\            $$\                     
      \__$$  __|                                     $$ |$$ |           $$ |                    
         $$ | $$$$$$\   $$$$$$$\  $$$$$$$\  $$$$$$\  $$ |$$ | $$$$$$\ $$$$$$\    $$$$$$\        
         $$ |$$  __$$\ $$  _____|$$  _____|$$  __$$\ $$ |$$ | \____$$\\_$$  _|  $$  __$$\       
         $$ |$$$$$$$$ |\$$$$$$\  \$$$$$$\  $$$$$$$$ |$$ |$$ | $$$$$$$ | $$ |    $$$$$$$$ |      
         $$ |$$   ____| \____$$\  \____$$\ $$   ____|$$ |$$ |$$  __$$ | $$ |$$\ $$   ____|      
         $$ |\$$$$$$$\ $$$$$$$  |$$$$$$$  |\$$$$$$$\ $$ |$$ |\$$$$$$$ | \$$$$  |\$$$$$$$\       
         \__| \_______|\_______/ \_______/  \_______|\__|\__| \_______|  \____/  \_______|      
                                                                                                

# Repository Overview

This repository contains two complementary spatial analytics pipelines powered by **DuckDB + Spatial**:

1) **Multi-State Branch & Census Radius Analytics** — estimates population within **3/5/10 miles** of each branch, assigns each branch to its containing **census tract**, and exports summary/detail outputs.

2) **Customer → Census Tract Join** — assigns each customer point to the **tract polygon** that contains it and writes Parquet/CSV outputs.

Both workflows use U.S. Census **TIGER/Line** tract shapefiles and **FFIEC** census flat files (population, CRA flags, urban/rural indicator).

---

## 1) Multi-State Branch & Census Radius Analytics
**Script:** `branch_tract_population_multi_state.py` (latest code)

### What it does
1. **Setup**
   - Creates a persistent DuckDB database and loads the `spatial` extension.
   - Drops intermediate tables/views from prior runs.

2. **Load Tracts**
   - Reads all TIGER/Line tract shapefile **ZIPs** from `zips/` via GDAL `/vsizip/`.
   - Appends into a single `tracts` table.

3. **Representative Points (not true centroids)**
   - Builds `tract_point_on_surface_deg` using:
     - `ST_PointOnSurface(ST_MakeValid(geom))` → guarantees the point lies **inside** the tract.
     - Extracts `lat`/`lon` using `ST_Y`/`ST_X` (EPSG:4326).
   - Rationale: true centroids can fall **outside** irregular/multipart polygons.

4. **Load Branches**
   - Reads `input/branch_coords/branch_coords.csv` (columns: `branch_id`, `lat`, `lon`).
   - Validates ranges; writes `branches_clean`.

5. **Load Census (FFIEC)**
   - Ingests `input/census/census_2025.csv` **(no header)** as VARCHAR.
   - Normalizes:
     - `state_fip` (2), `county_fip` (3), **`tract`** (6; implied decimal removed and left-padded).
     - Builds 11-digit **`geoid`** = state(2)+county(3)+tract(6).
   - Creates `tract_pop_geoid` with:
     - `population`, CRA indicators/classes, and **`urban_rural_ind`** (`U`, `R`, `M`, `I`).

6. **Radius Approximation (Haversine)**
   - Defines `haversine_m(lat1, lon1, lat2, lon2)` (meters).
   - Creates `radii_miles` (3, 5, 10 miles → meters).
   - `build_tracts_within_radius`: for each `branch_id × miles`, includes a tract if its **representative point** falls within the radius.
     - Joins: `branches_clean × radii_miles × tract_point_on_surface_deg`
     - Condition: `haversine_m(b.lat, b.lon, t.lat, t.lon) <= r.meters`

7. **Aggregation**
   - `branch_tract_counts`: counts tracts per `branch_id × miles`.
   - `branch_pop_tract`: sums `population` from `tract_pop_geoid` across included tracts.
   - `branch_pop_tract_wide`: pivots to one row per branch with `pop_3mi`, `pop_5mi`, `pop_10mi`.

8. **Branch → Tract Membership (Exact Polygon Containment)**
   - `build_branch_tract_membership`:
     - Assigns each branch to its **containing tract** with
       `ST_Within(ST_Transform(ST_Point(lon,lat), 'EPSG:4326','EPSG:4269'), ST_MakeValid(geom))`.
   - `build_branch_tract_membership_with_urban_rural`:
     - Enriches membership with `pop_3mi/5mi/10mi` from `branch_pop_tract_wide` and `urban_rural_ind` from `tract_pop_geoid`.
     - Uses `ROW_NUMBER()` tie-break to ensure **one row per branch** (if duplicates arise).

9. **Outputs**
   - Parquet/CSV written to `output/`:
     - `branch_pop_tract_wide` — population at 3/5/10 miles per branch  
     - `branch_pop_tract` — long-form population by `branch_id × miles`  
     - `branch_tract_counts` — tract counts by `branch_id × miles`  
     - `tracts_within_branch_radius_detail` — branch × miles × tract metadata + population  
     - `branch_tract_membership_with_pop` — branch’s containing tract + 3/5/10 mi population  
     - `branch_tract_membership_with_pop_urban` — same as above + **`urban_rural_ind`** + **`ALAND`** + **`AWATER`**

10. **Sanity Checks**
    - Prints row counts for key tables and optionally shows a small sample (`SAMPLE_TABLE`).

### Inputs
- **Branches CSV** (`BRANCH_COORD_CSV`): `branch_id`, `lat`, `lon`  
- **TIGER tract ZIPs** (`TIGER_DIR`): files like `tl_2024_XX_tract.zip`  
- **FFIEC census flat file** (`CENSUS_CSV`): no header; includes population, CRA fields, `urban_rural_ind`

### Key Tables & Views
- `tracts` — TIGER tracts (union of all ZIPs)  
- `tract_point_on_surface_deg` — representative point per tract (`lat`, `lon`, EPSG:4326)  
- `branches_clean` — validated branch points  
- `tract_pop_geoid` — FFIEC demographics by `geoid` (population, `urban_rural_ind`, CRA fields)  
- `tracts_within_branch_radius` — tracts whose rep-point lies within each branch’s 3/5/10 mi radius  
- `branch_tract_counts`, `branch_pop_tract`, `branch_pop_tract_wide` — counts & population summaries  
- `tracts_within_branch_radius_detail` — detailed rows with tract metadata + population  
- `branch_tract_membership` — branch → containing tract (polygon containment)  
- `branch_tract_membership_with_pop`, `branch_tract_membership_with_pop_urban` — membership enriched with 3/5/10 mi populations and `urban_rural_ind`

### Notes & Caveats
- **Representative point vs. centroid**: we use `ST_PointOnSurface` to ensure the reference point lies **inside** the tract. True centroids can fall outside complex shapes.  
- **Radius logic is an approximation**: it selects tracts whose **rep-point** is within the radius; it does not compute exact circle–polygon overlap.  
- **CRS alignment matters**: polygon containment transforms branch points from EPSG:4326 → EPSG:4269 to match TIGER geometries.  
- **Join keys**: always use **`geoid`** (state 2 + county 3 + tract 6) for robust joins across sources.

---

## 2) Customer → Census Tract Join
**Script:** `customer_tract_matching.py`

### What it does
1. Opens DuckDB and loads the spatial extension.
2. Reads **customers** from a Parquet file with `customer_id`, `latitude`, `longitude`.
3. Loads one or many TIGER tract ZIPs and builds a single `tracts` table.
4. Reprojects tract geometry from **EPSG:4269** → **EPSG:4326** and exposes a view `tracts_4326`.
5. Builds a `customers` table and a `customers_geom` view (WGS84 points).
6. Performs a **point-in-polygon** join to produce `customers_with_tract` (customer id, coordinates, tract identifiers).
7. Writes outputs:
   - Parquet with geometry: `output/customers_with_tract.parquet`
   - CSV without geometry: `output/customers_with_tract.csv`
8. Prints simple counts for customers read and joined.

### Inputs
- **Parquet** (`PARQUET_PATH`): columns `customer_id`, `latitude`, `longitude`  
- **TIGER tract ZIPs** (`TIGER_DIR`): files like `tl_2024_XX_tract.zip`

### Key Tables & Views
- `tracts` — raw TIGER tracts from ZIPs  
- `tracts_4326` — view with `state_fips`, `county_fips`, `ce_tract`, `geo_id`, geometry in EPSG:4326  
- `customers` — normalized ids and numeric `lat`, `lon`  
- `customers_geom` — WGS84 points  
- `customers_with_tract` — final join with customer id, coordinates, tract ids, and geometry

### Output Schemas
**customers_with_tract.parquet**
- `customer_id`, `lat`, `lon`, `state_fips`, `county_fips`, `ce_tract`, `geo_id`, `customer_geom` (EPSG:4326)

**customers_with_tract.csv**
- Same as above **minus** the geometry column.

---

## Running the Pipelines

- **Branch pipeline** (multi-state analytics):  
  Configure `DB_PATH`, `BRANCH_COORD_CSV`, `TIGER_DIR`, `CENSUS_CSV` and run the script.  
  Set `WRITE_EXPORTS=True` to emit Parquet/CSV in `output/`.

- **Customer join**:  
  Set `PARQUET_PATH` and `TIGER_DIR`, then run `customer_tract_matching.py`.

---

## References
- **FFIEC Census Flat Files** — https://www.ffiec.gov/data/census/flat-files
- **FFIEC Census Data Dictionary** — https://www.ffiec.gov/data/census/  
- **TIGER/Line Shapefiles** — https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html
- **TIGER/Line PDF Doc** — https://www2.census.gov/geo/pdfs/maps-data/data/tiger/tgrshp2025/TGRSHP2025_TechDoc.pdf

---

## Requirements
- **DuckDB** with **spatial** extension (installed/loaded by the scripts)
- Platypus dependency 




