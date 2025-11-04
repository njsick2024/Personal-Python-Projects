# =============================================================================
# Customers Within Isochrones Pipeline
# - Functional refactor
# - Paths via pathlib
# - Iterations via itertools
# - Context managers and exception handling
# - Minimal dependencies and clear separation of concerns
# =============================================================================

from __future__ import annotations

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

# =============================================================================
# Configuration
# =============================================================================


# Create a centralized configuration object
class Config:
    branch_csv: Path
    client_csv: Path
    out_dir: Path
    api_key: str
    profile: str = "driving"
    time_intervals_min: Tuple[int, ...] = (10,)
    verify_ssl: bool = True
    ignore_system_proxies: bool = True
    output_csv_name: str = "clients_within_isos_timeframes.csv"


# Build a config and ensure output directory exists
def make_config(
    branch_csv: Path,
    client_csv: Path,
    out_dir: Path,
    api_key: str,
    profile: str = "driving",
    time_intervals_min: Tuple[int, ...] = (10,),
    verify_ssl: bool = True,
    ignore_system_proxies: bool = True,
) -> Config:
    out_dir.mkdir(parents=True, exist_ok=True)
    return Config(
        branch_csv=branch_csv,
        client_csv=client_csv,
        out_dir=out_dir,
        api_key=api_key,
        profile=profile,
        time_intervals_min=time_intervals_min,
        verify_ssl=verify_ssl,
        ignore_system_proxies=ignore_system_proxies,
    )


# =============================================================================
# IO and clients
# =============================================================================


# Build a requests session with TLS and proxy controls
@cache
def build_session(verify_ssl: bool, ignore_system_proxies: bool) -> requests.Session:
    sess = requests.Session()
    sess.verify = verify_ssl
    if ignore_system_proxies:
        sess.trust_env = False
        sess.proxies = {}
    return sess


# Create a Mapbox router client instance
@cache
def make_router(api_key: str) -> MapboxOSRM:
    return MapboxOSRM(api_key=api_key)


# Load branch and customer input CSVs
def load_inputs(cfg: Config) -> Tuple[pd.DataFrame, pd.DataFrame]:
    with cfg.branch_csv.open("r", encoding="utf-8") as f:
        df_bc = pd.read_csv(f)
    with cfg.client_csv.open("r", encoding="utf-8") as f:
        df_cust = pd.read_csv(f)

    df_cust = df_cust.dropna(subset=["cust_lat", "cust_long"])
    df_bc[["latitude", "longitude"]] = df_bc[["latitude", "longitude"]].astype(float)
    df_cust[["cust_lat", "cust_long"]] = df_cust[["cust_lat", "cust_long"]].astype(float)

    return df_bc.reset_index(drop=True), df_cust.reset_index(drop=True)


# Convert DataFrames to GeoDataFrames
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


# =============================================================================
# Core geospatial logic
# =============================================================================


# Generate isochrone polygons for each branch and time interval
def generate_isochrones(cfg: Config, gdf_bc: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    router = make_router(cfg.api_key)
    intervals_sec = [m * 60 for m in cfg.time_intervals_min]
    rows: List[Dict] = []

    for _, bc_row in gdf_bc.iterrows():
        coord = [float(bc_row["longitude"]), float(bc_row["latitude"])]
        bc_code = bc_row["bc_code"]
        for minutes, seconds in product(cfg.time_intervals_min, intervals_sec):
            try:
                resp = router.isochrones(
                    locations=coord,
                    profile=cfg.profile,
                    intervals=[seconds],
                    polygons="true",
                )
            except Exception as exc:
                print(f"[WARN] Isochrone request failed bc_code={bc_code} minutes={minutes} err={exc}")
                continue

            for iso in resp:
                try:
                    poly = Polygon(iso.geometry[0])
                except Exception as exc:
                    print(f"[WARN] Invalid polygon bc_code={bc_code} minutes={minutes} err={exc}")
                    continue

                rows.append(
                    {
                        "bc_code": bc_code,
                        "branch_latitude": float(bc_row["latitude"]),
                        "branch_longitude": float(bc_row["longitude"]),
                        "time_frame_minutes": int(minutes),
                        "geometry": poly,
                    }
                )

    return gpd.GeoDataFrame(rows, crs="EPSG:4326")


# Match customers that fall within any isochrone
def customers_within(gdf_iso: gpd.GeoDataFrame, gdf_cust: gpd.GeoDataFrame) -> pd.DataFrame:
    out_frames: List[pd.DataFrame] = []
    for _, iso_row in gdf_iso.iterrows():
        poly = iso_row["geometry"]
        bc_code = iso_row["bc_code"]
        minutes = iso_row["time_frame_minutes"]

        try:
            mask = gdf_cust.geometry.within(poly)
            subset = gdf_cust.loc[mask]
        except Exception as exc:
            print(f"[WARN] Spatial within failed bc_code={bc_code} minutes={minutes} err={exc}")
            continue

        if subset.empty:
            continue

        part = pd.DataFrame(
            {
                "bc_code": bc_code,
                "branch_latitude": iso_row["branch_latitude"],
                "branch_longitude": iso_row["branch_longitude"],
                "customer_id": subset["customer_id"].values,
                "cust_lat": subset["cust_lat"].values,
                "cust_long": subset["cust_long"].values,
                "driving_time_minutes": minutes,
            }
        )
        out_frames.append(part)

    if not out_frames:
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
    return pd.concat(out_frames, ignore_index=True)


# =============================================================================
# Persistence
# =============================================================================


# Save the results to CSV
def save_results_csv(cfg: Config, df: pd.DataFrame) -> Optional[Path]:
    try:
        out_path = cfg.out_dir / cfg.output_csv_name
        df.to_csv(out_path, index=False)
        return out_path
    except Exception as exc:
        print(f"[WARN] Failed to write CSV err={exc}")
        return None


# =============================================================================
# Orchestration
# =============================================================================


# Run the end to end pipeline
def run(cfg: Config) -> pd.DataFrame:
    df_bc, df_cust = load_inputs(cfg)
    gdf_bc, gdf_cust = to_geodataframes(df_bc, df_cust)
    gdf_iso = generate_isochrones(cfg, gdf_bc)
    results = customers_within(gdf_iso, gdf_cust)
    save_results_csv(cfg, results)
    return results


# =============================================================================
# Entry point
# =============================================================================

if __name__ == "__main__":
    cfg = make_config(
        branch_csv=Path("data/branch_lat_lon.csv"),
        client_csv=Path("data/client_lat_lon.csv"),
        out_dir=Path("output"),
        api_key="",  # set your Mapbox token
        profile="driving",
        time_intervals_min=(10,),
        verify_ssl=True,
        ignore_system_proxies=True,
    )
    df_out = run(cfg)
    print(df_out.head())
