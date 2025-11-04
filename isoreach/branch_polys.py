# =====================================================================================
# Geospatial Isochrone Pipeline
# - Functional refactor with clear notes
# - Paths via pathlib
# - Iterations via itertools
# - Context managers and exception handling
# - Optional TLS verify toggle
# - Outputs in GeoJSON, Parquet, CSV
# =====================================================================================

from __future__ import annotations

import json
import warnings
from dataclasses import dataclass
from functools import cache
from itertools import product
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import geopandas as gpd
import pandas as pd
import requests
from routingpy.routers import MapboxOSRM
from shapely.geometry import Polygon

warnings.filterwarnings("ignore")


# =====================================================================================
# Config
# =====================================================================================


@dataclass(frozen=True)
class Config:
    branch_csv: Path
    client_csv: Path
    out_dir: Path
    mapbox_api_key: str
    profile: str
    time_intervals_min: Tuple[int, ...]
    verify_ssl: bool = True
    ignore_system_proxies: bool = True
    isochrones_geojson_name: str = "banking_center_10_min_isos.geojson"
    clients_within_name_parquet: str = "personal_clients_within_10_min_iso_timeframe.parquet"
    clients_within_name_csv: str = "personal_clients_within_10_min_iso_timeframe.csv"


# =====================================================================================
# Helpers
# =====================================================================================


def make_config(
    branch_csv: Path,
    client_csv: Path,
    out_dir: Path,
    mapbox_api_key: str,
    profile: str = "driving",
    time_intervals_min: Tuple[int, ...] = (10,),
    verify_ssl: bool = True,
    ignore_system_proxies: bool = True,
) -> Config:
    # ensure output directory exists
    out_dir.mkdir(parents=True, exist_ok=True)
    return Config(
        branch_csv=branch_csv,
        client_csv=client_csv,
        out_dir=out_dir,
        mapbox_api_key=mapbox_api_key,
        profile=profile,
        time_intervals_min=time_intervals_min,
        verify_ssl=verify_ssl,
        ignore_system_proxies=ignore_system_proxies,
    )


# Build requests session
def build_session(verify_ssl: bool, ignore_system_proxies: bool) -> requests.Session:
    session = requests.Session()
    session.verify = verify_ssl
    if ignore_system_proxies:
        session.trust_env = False
        session.proxies = {}
    return session


# Create a Mapbox OSRM client
def make_mapbox_router(api_key: str) -> MapboxOSRM:
    return MapboxOSRM(api_key=api_key)


# Load branch and client CSVs
def load_input_data(cfg: Config) -> Tuple[pd.DataFrame, pd.DataFrame]:
    with cfg.branch_csv.open("r", encoding="utf-8") as f:
        df_bc = pd.read_csv(f)
    with cfg.client_csv.open("r", encoding="utf-8") as f:
        df_cust = pd.read_csv(f)

    # drop customers with missing coordinates
    df_cust = df_cust.dropna(subset=["cust_lat", "cust_long"])

    # ensure numeric types for coordinates
    df_bc[["latitude", "longitude"]] = df_bc[["latitude", "longitude"]].astype(float)
    df_cust[["cust_lat", "cust_long"]] = df_cust[["cust_lat", "cust_long"]].astype(float)

    # sort as a stable habit for reproducibility
    df_bc = df_bc.sort_values(by="latitude", ascending=True).reset_index(drop=True)
    df_cust = df_cust.sort_values(by="cust_lat", ascending=True).reset_index(drop=True)
    return df_bc, df_cust


# Convert pandas frames to GeoDataFrames with EPSG 4326
def to_geodataframes(df_bc: pd.DataFrame, df_cust: pd.DataFrame) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    gdf_bc = gpd.GeoDataFrame(
        df_bc,
        geometry=gpd.points_from_xy(df_bc.longitude, df_bc.latitude),
        crs="EPSG:4326",
    )
    gdf_cust = gpd.GeoDataFrame(
        df_cust,
        geometry=gpd.points_from_xy(df_cust.cust_long, df_cust.cust_lat),
        crs="EPSG:4326",
    )
    return gdf_bc, gdf_cust


# Generate isochrones for all branches and requested time intervals
def generate_isochrones(cfg: Config, gdf_bc: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    router = make_mapbox_router(cfg.mapbox_api_key)
    intervals_sec = [m * 60 for m in cfg.time_intervals_min]
    rows: List[Dict] = []

    # Loop over Cartesian product of branches and time intervals
    for _, bc_row in gdf_bc.iterrows():
        coord = [float(bc_row["longitude"]), float(bc_row["latitude"])]
        bc_code = bc_row["bc_code"]
        for time_min, time_sec in product(cfg.time_intervals_min, intervals_sec):
            try:
                iso_resp = router.isochrones(
                    locations=coord,
                    profile=cfg.profile,
                    intervals=[time_sec],
                    polygons="true",
                    dry_run=False,
                )
            except Exception as exc:
                print(f"[WARN] Isochrone request failed bc_code={bc_code} minutes={time_min} err={exc}")
                continue

            for iso in iso_resp:
                try:
                    poly = Polygon(iso.geometry[0])
                except Exception as exc:
                    print(f"[WARN] Polygon build failed bc_code={bc_code} minutes={time_min} err={exc}")
                    continue

                rows.append(
                    {
                        "bc_code": bc_code,
                        "branch_latitude": float(bc_row["latitude"]),
                        "branch_longitude": float(bc_row["longitude"]),
                        "geometry": poly,
                        "time_frame_minutes": int(time_min),
                    }
                )

    gdf_iso = gpd.GeoDataFrame(rows, crs="EPSG:4326")
    return gdf_iso


# Select customers that fall within each isochrone polygon
def customers_within_isochrones(gdf_iso: gpd.GeoDataFrame, gdf_cust: gpd.GeoDataFrame) -> pd.DataFrame:
    results: List[Dict] = []
    for _, iso_row in gdf_iso.iterrows():
        poly = iso_row["geometry"]
        bc_code = iso_row["bc_code"]
        time_min = iso_row["time_frame_minutes"]

        try:
            mask = gdf_cust.geometry.within(poly)
            clients = gdf_cust.loc[mask]
        except Exception as exc:
            print(f"[WARN] Spatial within failed bc_code={bc_code} minutes={time_min} err={exc}")
            continue

        if not clients.empty:
            part = pd.DataFrame(
                {
                    "bc_code": bc_code,
                    "branch_latitude": iso_row["branch_latitude"],
                    "branch_longitude": iso_row["branch_longitude"],
                    "customer_id": clients["customer_id"].values,
                    "cust_lat": clients["cust_lat"].values,
                    "cust_long": clients["cust_long"].values,
                    "driving_time_minutes": time_min,
                }
            )
            results.append(part)

    if results:
        return pd.concat(results, ignore_index=True)
    return pd.DataFrame(
        columns=[
            "bc_code",
            "branch_latitude",
            "branch_longitude",
            "customer_id",
            "cust_lat",
            "cust_long",
            "driving_time_minutes",
        ]
    )


# Persist isochrones GeoDataFrame to GeoJSON
def save_isochrones_geojson(cfg: Config, gdf_iso: gpd.GeoDataFrame) -> Path:
    out_fp = cfg.out_dir / cfg.isochrones_geojson_name
    try:
        gdf_iso.to_file(out_fp, driver="GeoJSON")
    except Exception as exc:
        print(f"[WARN] Failed to write GeoJSON err={exc}")
    return out_fp


# Persist customer matches to Parquet and CSV
def save_customer_matches(cfg: Config, df_matches: pd.DataFrame) -> Tuple[Path, Path]:
    out_parquet = cfg.out_dir / cfg.clients_within_name_parquet
    out_csv = cfg.out_dir / cfg.clients_within_name_csv
    try:
        df_matches.to_parquet(out_parquet, engine="pyarrow", index=False)
    except Exception as exc:
        print(f"[WARN] Failed to write Parquet err={exc}")
    try:
        df_matches.to_csv(out_csv, index=False)
    except Exception as exc:
        print(f"[WARN] Failed to write CSV err={exc}")
    return out_parquet, out_csv


# Compute overlap percentages between isochrone polygons
def compute_isochrone_overlaps(gdf_iso: gpd.GeoDataFrame) -> pd.DataFrame:
    rows: List[Tuple[str, str, float]] = []
    for i, row_i in gdf_iso.iterrows():
        bc_i = row_i["bc_code"]
        poly_i = row_i["geometry"]
        for j, row_j in gdf_iso.iterrows():
            bc_j = row_j["bc_code"]
            poly_j = row_j["geometry"]
            try:
                inter = poly_i.intersection(poly_j)
                area_i = poly_i.area
                area_j = poly_j.area
                denom = area_i + area_j
                pct = float((inter.area / denom) * 100.0) if denom > 0 else 0.0
            except Exception:
                pct = 0.0
            rows.append((bc_i, bc_j, pct))
    return pd.DataFrame(rows, columns=["bc_code_1", "bc_code_2", "overlap_percentage"])


# Convert GeoJSON of isochrones to an ordered polygon vertex CSV per branch
def geojson_to_ordered_csv(geojson_path: Path, out_csv: Path, time_value: int) -> Optional[Path]:
    try:
        with geojson_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as exc:
        print(f"[WARN] Failed to read GeoJSON err={exc}")
        return None

    rows: List[Dict] = []
    for feature in data.get("features", []):
        props = feature.get("properties", {})
        geom = feature.get("geometry", {})
        coords = geom.get("coordinates", [])
        if not coords:
            continue
        ring = coords[0]
        bc_code = props.get("bc_code")
        branch_lat = props.get("branch_latitude")
        branch_lon = props.get("branch_longitude")
        for lon, lat in ring:
            rows.append(
                {
                    "bc_code": bc_code,
                    "branch_latitude": branch_lat,
                    "branch_longitude": branch_lon,
                    "lon_poly": lon,
                    "lat_poly": lat,
                    "time_poly": time_value,
                }
            )
    if not rows:
        return None
    df = pd.DataFrame(rows)
    df["order"] = df.groupby("bc_code").cumcount() + 1
    try:
        df.to_csv(out_csv, index=False)
    except Exception as exc:
        print(f"[WARN] Failed to write ordered polygon CSV err={exc}")
        return None
    return out_csv


# Merge multiple CSV files
def merge_csvs(inputs: Iterable[Path], out_csv: Path) -> Optional[Path]:
    frames: List[pd.DataFrame] = []
    for fp in inputs:
        if not fp.exists():
            print(f"[WARN] Missing CSV {fp}")
            continue
        try:
            frames.append(pd.read_csv(fp))
        except Exception as exc:
            print(f"[WARN] Failed to read CSV {fp} err={exc}")
    if not frames:
        return None
    merged = pd.concat(frames, ignore_index=True)
    try:
        merged.to_csv(out_csv, index=False)
    except Exception as exc:
        print(f"[WARN] Failed to write merged CSV err={exc}")
        return None
    return out_csv


# Compute customer counts and percentages per branch code
def compute_customer_percentages(df_matches: pd.DataFrame) -> pd.DataFrame:
    if df_matches.empty:
        return df_matches.copy()
    # counts per branch for each row
    counts = df_matches.groupby("bc_code")["customer_id"].transform("count")
    total_unique = df_matches["customer_id"].nunique()
    pct = (counts / total_unique) * 100.0 if total_unique > 0 else 0.0

    out = df_matches.copy()
    out["count_of_customers"] = counts
    out["percentage_of_customers"] = pct
    return out[
        [
            "bc_code",
            "percentage_of_customers",
            "count_of_customers",
            "customer_id",
            "branch_latitude",
            "branch_longitude",
            "cust_lat",
            "cust_long",
            "driving_time_minutes",
        ]
    ]


# Optional Mapbox Matrix for source destination pairs
def mapbox_matrix_for_pairs(coord_pairs: Iterable[str], api_key: str, verify_ssl: bool) -> pd.DataFrame:
    sess = build_session(verify_ssl=verify_ssl, ignore_system_proxies=True)
    all_rows: List[Dict] = []
    for coords in coord_pairs:
        url = (
            f"https://api.mapbox.com/directions-matrix/v1/mapbox/driving/"
            f"{coords}?approaches=curb;curb&access_token={api_key}"
        )
        try:
            with sess.get(url, timeout=60) as resp:
                resp.raise_for_status()
                data = resp.json()
        except Exception as exc:
            print(f"[WARN] Matrix request failed coords={coords} err={exc}")
            continue

        dests = [
            {"name": d.get("name", "Unknown"), "location": d["location"], "distance": d.get("distance")}
            for d in data.get("destinations", [])
        ]
        srcs = [
            {"name": s.get("name", "Unknown"), "location": s["location"], "distance": s.get("distance")}
            for s in data.get("sources", [])
        ]
        durations = data.get("durations") or []

        for i, src in enumerate(srcs):
            for j, dst in enumerate(dests):
                dur = durations[i][j] if i < len(durations) and j < len(durations[i]) else None
                all_rows.append(
                    {
                        "source_name": src["name"],
                        "source_longitude": src["location"][0],
                        "source_latitude": src["location"][1],
                        "source_distance": src.get("distance"),
                        "destination_name": dst["name"],
                        "destination_longitude": dst["location"][0],
                        "destination_latitude": dst["location"][1],
                        "destination_distance": dst.get("distance"),
                        "duration": dur,
                    }
                )
    return pd.DataFrame(all_rows)


# Main execution function
def run(cfg: Config) -> None:
    df_bc, df_cust = load_input_data(cfg)
    gdf_bc, gdf_cust = to_geodataframes(df_bc, df_cust)

    gdf_iso = generate_isochrones(cfg, gdf_bc)
    iso_geojson_fp = save_isochrones_geojson(cfg, gdf_iso)

    df_matches = customers_within_isochrones(gdf_iso, gdf_cust)
    matches_parquet_fp, matches_csv_fp = save_customer_matches(cfg, df_matches)

    overlaps_df = compute_isochrone_overlaps(gdf_iso)
    overlaps_csv_fp = cfg.out_dir / "overlapping_bc_percentage.csv"
    try:
        overlaps_df.to_csv(overlaps_csv_fp, index=False)
    except Exception as exc:
        print(f"[WARN] Failed to write overlaps CSV err={exc}")

    ordered_csv_fp = cfg.out_dir / "small_business_output_with_order_10_min.csv"
    geojson_to_ordered_csv(iso_geojson_fp, ordered_csv_fp, time_value=cfg.time_intervals_min[0])

    # percentage summary for current interval
    customer_pct_df = compute_customer_percentages(df_matches)
    pct_parquet = cfg.out_dir / f"clients_with_percentage_{cfg.time_intervals_min[0]}_min.parquet"
    pct_csv = cfg.out_dir / f"clients_with_percentage_{cfg.time_intervals_min[0]}_min.csv"
    try:
        customer_pct_df.to_parquet(pct_parquet, engine="pyarrow", index=False)
    except Exception as exc:
        print(f"[WARN] Failed to write percentage Parquet err={exc}")
    try:
        customer_pct_df.to_csv(pct_csv, index=False)
    except Exception as exc:
        print(f"[WARN] Failed to write percentage CSV err={exc}")


if __name__ == "__main__":
    cfg = make_config(
        branch_csv=Path("data/branch_lat_lon.csv"),
        client_csv=Path("data/client_lat_lon.csv"),
        out_dir=Path("output"),
        mapbox_api_key="",  # set your Mapbox token
        profile="driving",
        time_intervals_min=(10,),
        verify_ssl=True,  # set False only while validating trust chain
        ignore_system_proxies=True,  # set False if you need corporate proxy from env
    )
    run(cfg)
