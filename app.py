
from __future__ import annotations

import ast
import base64
import csv
import difflib
import io
import json
import math
import os
import re
import sqlite3
import threading
import zipfile
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request
from pypdf import PdfReader
from shapely.geometry import Point, Polygon, box, shape, mapping
from shapely.ops import unary_union

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "scorecard.db"
CACHE_DIR = BASE_DIR / "cache"
CACHE_DIR.mkdir(exist_ok=True)
REGION_METRICS_JSON = "region_metrics.json"
SOURCES_REGISTRY_JSON = "sources_registry.json"

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

HTTP_HEADERS = {
    "User-Agent": "VisionOneMillionScorecardBot/10.0 (hackathon prototype)",
    "Accept": "application/json,text/html,application/pdf,text/csv,application/zip,*/*",
}

REGION_BBOX = {"south": 43.245, "west": -80.68, "north": 43.61, "east": -80.14}
DEFAULT_GRID_STEP = 0.022  # bigger, stable tiles
LOCAL_TRAFFIC_GEOJSON_CANDIDATES = [
    BASE_DIR / "data" / "Traffic_Volumes.geojson",
    BASE_DIR / "Traffic_Volumes.geojson",
]
CITY_NAMES = ["Kitchener", "Waterloo", "Cambridge"]
CITY_FALLBACK_BBOXES = {
    "Kitchener": {"west": -80.58, "south": 43.39, "east": -80.40, "north": 43.51},
    "Waterloo": {"west": -80.61, "south": 43.43, "east": -80.47, "north": 43.53},
    "Cambridge": {"west": -80.42, "south": 43.30, "east": -80.24, "north": 43.43},
}
RUN_LOCK = threading.Lock()
CURRENT_RUN_ID: int | None = None
CURRENT_THREAD: threading.Thread | None = None

SECTORS = ["Housing", "Transportation", "Healthcare", "Employment", "Placemaking"]
DEFAULT_SECTOR_WEIGHTS = {s: 1.0 for s in SECTORS}

DEFAULT_SOURCES = [
    {"name": "GRT Open Data", "sector": "Transportation", "source_url": "https://www.grt.ca/en/about-grt/open-data.aspx", "access_method": "html", "parser_type": "html", "update_frequency": "weekly", "use_llm": 0, "notes": "Official GRT open data landing page."},
    {"name": "GRT GTFS static feed", "sector": "Transportation", "source_url": "https://webapps.regionofwaterloo.ca/api/grt-routes/api/staticfeeds/1", "access_method": "gtfs", "parser_type": "zip/gtfs", "update_frequency": "daily", "use_llm": 0, "notes": "Official GTFS feed."},
    {"name": "Traffic Volumes", "sector": "Transportation", "source_url": "https://rowopendata-rmw.opendata.arcgis.com/items/426089c5166c4f8f8f4000acb2fef840", "access_method": "geojson", "parser_type": "geojson", "update_frequency": "annual", "use_llm": 0, "notes": "Regional traffic volumes / AADT."},
    {"name": "Region hospitals page", "sector": "Healthcare", "source_url": "https://www.regionofwaterloo.ca/en/living-here/hospitals.aspx", "access_method": "html", "parser_type": "html", "update_frequency": "weekly", "use_llm": 0, "notes": "Official hospital list."},
    {"name": "WRHN emergency departments", "sector": "Healthcare", "source_url": "https://www.wrhn.ca/healthcare-services/emergency-departments", "access_method": "html", "parser_type": "html", "update_frequency": "weekly", "use_llm": 0, "notes": "WRHN emergency departments."},
    {"name": "Ontario emergency wait page", "sector": "Healthcare", "source_url": "https://www.ontario.ca/page/time-spent-emergency-department", "access_method": "html", "parser_type": "html", "update_frequency": "monthly", "use_llm": 0, "notes": "Emergency wait time page."},
    {"name": "ER Watch Ontario", "sector": "Healthcare", "source_url": "https://www.er-watch.ca/", "access_method": "html", "parser_type": "html", "update_frequency": "hourly", "use_llm": 0, "notes": "Real-time ER wait times; matched to KCW hospitals."},
    {"name": "CMHC market profile - KCW", "sector": "Housing", "source_url": "https://www03.cmhc-schl.gc.ca/hmip-pimh/en/Profile?a=20&geoId=0850&t=3", "access_method": "html", "parser_type": "html", "update_frequency": "monthly", "use_llm": 0, "notes": "CMHC KCW market profile."},
    {"name": "CMHC KCW full table", "sector": "Housing", "source_url": "https://www03.cmhc-schl.gc.ca/hmip-pimh/en/TableMapChart?id=0850&t=3", "access_method": "html", "parser_type": "html", "update_frequency": "annual", "use_llm": 0, "notes": "CMHC KCW full housing table."},
    {"name": "WRAR market stats", "sector": "Housing", "source_url": "https://wrar.ca/category/market-stats/", "access_method": "html", "parser_type": "html", "update_frequency": "monthly", "use_llm": 0, "notes": "WRAR market-stats archive."},
    {"name": "Statistics Canada WDS", "sector": "Employment", "source_url": "https://www.statcan.gc.ca/en/developers/wds/user-guide", "access_method": "api", "parser_type": "json", "update_frequency": "monthly", "use_llm": 0, "notes": "StatCan WDS documentation."},
    {"name": "StatsCan labour table 14-10-0459-01", "sector": "Employment", "source_url": "https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1410045901", "access_method": "csv", "parser_type": "csv/zip", "update_frequency": "monthly", "use_llm": 0, "notes": "Seasonally adjusted CMA labour data."},
    {"name": "StatsCan city profiles", "sector": "Employment", "source_url": "https://www12.statcan.gc.ca/census-recensement/2021/as-sa/fogs-spg/page.cfm", "access_method": "html", "parser_type": "html", "update_frequency": "annual", "use_llm": 0, "notes": "City profile pages for current city-level labour and density."},
    {"name": "OSM amenities and trails", "sector": "Placemaking", "source_url": "https://overpass-api.de/api/interpreter", "access_method": "api", "parser_type": "json", "update_frequency": "weekly", "use_llm": 0, "notes": "Amenities and trails from OpenStreetMap."},
    {"name": "OSM city boundaries", "sector": "Placemaking", "source_url": "https://nominatim.openstreetmap.org/search", "access_method": "api", "parser_type": "geojson", "update_frequency": "monthly", "use_llm": 0, "notes": "Kitchener, Waterloo, and Cambridge boundaries."},
]

METRIC_DEFS = [
    {"metric_key": "transport_stop_density", "sector": "Transportation", "name": "Transit stop density", "unit": "stops", "direction": "higher", "min_good": 0.0, "max_good": 80.0, "weight": 1.0, "description": "Stops per area."},
    {"metric_key": "transport_avg_stop_distance_m", "sector": "Transportation", "name": "Average stop distance", "unit": "m", "direction": "lower", "min_good": 100.0, "max_good": 1200.0, "weight": 1.0, "description": "Distance to nearest transit stop."},
    {"metric_key": "transport_route_density", "sector": "Transportation", "name": "Routes nearby", "unit": "routes", "direction": "higher", "min_good": 0.0, "max_good": 12.0, "weight": 1.0, "description": "Distinct nearby routes."},
    {"metric_key": "transport_service_density", "sector": "Transportation", "name": "Service density", "unit": "trips", "direction": "higher", "min_good": 0.0, "max_good": 250.0, "weight": 0.9, "description": "Nearby scheduled trips."},
    {"metric_key": "transport_avg_aadt", "sector": "Transportation", "name": "Average AADT", "unit": "vehicles/day", "direction": "lower", "min_good": 2000.0, "max_good": 50000.0, "weight": 0.7, "description": "Average annual daily traffic."},
    {"metric_key": "transport_coverage_pct", "sector": "Transportation", "name": "Transit-access coverage", "unit": "%", "direction": "higher", "min_good": 20.0, "max_good": 95.0, "weight": 1.1, "description": "Share of cells scoring 60+."},
    {"metric_key": "healthcare_avg_wait_hours", "sector": "Healthcare", "name": "Emergency wait time", "unit": "hours", "direction": "lower", "min_good": 1.0, "max_good": 12.0, "weight": 1.2, "description": "Average emergency wait time."},
    {"metric_key": "healthcare_avg_hospital_distance_km", "sector": "Healthcare", "name": "Hospital distance", "unit": "km", "direction": "lower", "min_good": 0.5, "max_good": 12.0, "weight": 1.1, "description": "Distance to nearest hospital."},
    {"metric_key": "healthcare_doctors_count", "sector": "Healthcare", "name": "Doctors nearby", "unit": "count", "direction": "higher", "min_good": 0.0, "max_good": 60.0, "weight": 0.8, "description": "Doctors nearby."},
    {"metric_key": "healthcare_dentists_count", "sector": "Healthcare", "name": "Dentists nearby", "unit": "count", "direction": "higher", "min_good": 0.0, "max_good": 30.0, "weight": 0.6, "description": "Dentists nearby."},
    {"metric_key": "healthcare_clinics_count", "sector": "Healthcare", "name": "Clinics nearby", "unit": "count", "direction": "higher", "min_good": 0.0, "max_good": 30.0, "weight": 0.7, "description": "Clinics nearby."},
    {"metric_key": "housing_avg_rent_cad", "sector": "Housing", "name": "Average rent", "unit": "CAD", "direction": "lower", "min_good": 900.0, "max_good": 3200.0, "weight": 1.1, "description": "Average rent."},
    {"metric_key": "housing_vacancy_rate_pct", "sector": "Housing", "name": "Vacancy rate", "unit": "%", "direction": "higher", "min_good": 1.0, "max_good": 6.0, "weight": 0.9, "description": "Vacancy rate."},
    {"metric_key": "housing_median_price_cad", "sector": "Housing", "name": "Median home price", "unit": "CAD", "direction": "lower", "min_good": 300000.0, "max_good": 1200000.0, "weight": 1.0, "description": "Median home price."},
    {"metric_key": "housing_avg_rent_1br_cad", "sector": "Housing", "name": "Average rent (1 bedroom)", "unit": "CAD", "direction": "lower", "min_good": 800.0, "max_good": 2800.0, "weight": 0.7, "description": "CMHC average rent 1 bedroom KCW."},
    {"metric_key": "housing_avg_rent_3br_cad", "sector": "Housing", "name": "Average rent (3 bedroom)", "unit": "CAD", "direction": "lower", "min_good": 1200.0, "max_good": 3600.0, "weight": 0.7, "description": "CMHC average rent 3 bedroom KCW."},
    {"metric_key": "housing_active_listings_proxy", "sector": "Housing", "name": "Active listings (proxy)", "unit": "count", "direction": "higher", "min_good": 0.0, "max_good": 5000.0, "weight": 0.5, "description": "Proxy count from regional listing activity."},
    {"metric_key": "employment_rate_pct", "sector": "Employment", "name": "Employment rate", "unit": "%", "direction": "higher", "min_good": 50.0, "max_good": 80.0, "weight": 1.2, "description": "Employment rate."},
    {"metric_key": "unemployment_rate_pct", "sector": "Employment", "name": "Unemployment rate", "unit": "%", "direction": "lower", "min_good": 2.0, "max_good": 12.0, "weight": 1.0, "description": "Unemployment rate."},
    {"metric_key": "labour_participation_pct", "sector": "Employment", "name": "Labour participation", "unit": "%", "direction": "higher", "min_good": 55.0, "max_good": 80.0, "weight": 0.9, "description": "Participation rate."},
    {"metric_key": "placemaking_amenity_count", "sector": "Placemaking", "name": "Amenity count", "unit": "count", "direction": "higher", "min_good": 0.0, "max_good": 40.0, "weight": 0.9, "description": "Amenity count."},
    {"metric_key": "placemaking_amenity_diversity", "sector": "Placemaking", "name": "Amenity diversity", "unit": "types", "direction": "higher", "min_good": 0.0, "max_good": 8.0, "weight": 1.1, "description": "Amenity diversity."},
    {"metric_key": "placemaking_trail_km", "sector": "Placemaking", "name": "Trail length", "unit": "km", "direction": "higher", "min_good": 0.0, "max_good": 20.0, "weight": 0.8, "description": "Trail length."},
    {"metric_key": "placemaking_walk_links", "sector": "Placemaking", "name": "Walkability links", "unit": "count", "direction": "higher", "min_good": 0.0, "max_good": 40.0, "weight": 0.7, "description": "Trail/path count."},
    {"metric_key": "placemaking_amenity_mix_score", "sector": "Placemaking", "name": "Amenity mix score", "unit": "score", "direction": "higher", "min_good": 0.0, "max_good": 100.0, "weight": 0.8, "description": "Count + amenity type diversity (tiers)."},
    {"metric_key": "population_density_city", "sector": "Placemaking", "name": "Population density", "unit": "people/km²", "direction": "higher", "min_good": 500.0, "max_good": 7000.0, "weight": 0.5, "description": "Population density."},
]

DEFAULT_FORMULAS = [
    {
        "formula_key": "transport_score",
        "title": "Transportation score",
        "scope": "sector",
        "expression": "100*(0.20*clamp(stop_density/80,0,1)+0.20*clamp(route_density/12,0,1)+0.16*clamp(service_density/250,0,1)+0.18*clamp(1-avg_stop_distance_m/700,0,1)+0.12*clamp(1-avg_aadt/50000,0,1)+0.14*clamp(pop_density/7000,0,1))",
        "variables_json": json.dumps([
            {"name": "stop_density", "description": "Transit stops in the area."},
            {"name": "route_density", "description": "Distinct routes near the area."},
            {"name": "service_density", "description": "Nearby scheduled trips."},
            {"name": "avg_stop_distance_m", "description": "Average distance to nearest stop."},
            {"name": "avg_aadt", "description": "Average annual daily traffic."},
            {"name": "pop_density", "description": "City-level population density."},
        ]),
        "notes": "Used for transportation cell coloring.",
    },
    {
        "formula_key": "healthcare_score",
        "title": "Healthcare score",
        "scope": "sector",
        "expression": "100*(0.24*clamp(1-avg_hospital_distance_km/10,0,1)+0.26*clamp(1-avg_wait_hours/12,0,1)+0.18*clamp(doctors_count/60,0,1)+0.14*clamp(dentists_count/30,0,1)+0.18*clamp(clinics_count/30,0,1))",
        "variables_json": json.dumps([
            {"name": "avg_hospital_distance_km", "description": "Distance to nearest hospital."},
            {"name": "avg_wait_hours", "description": "Emergency wait time."},
            {"name": "doctors_count", "description": "Doctors count."},
            {"name": "dentists_count", "description": "Dentists count."},
            {"name": "clinics_count", "description": "Clinics count."},
        ]),
        "notes": "Used for healthcare cell coloring.",
    },
    {
        "formula_key": "housing_score",
        "title": "Housing score",
        "scope": "sector",
        "expression": "100*(0.45*clamp(1-rent_cad/3200,0,1)+0.2*clamp(vacancy_rate_pct/6,0,1)+0.35*clamp(1-home_price_cad/1200000,0,1))",
        "variables_json": json.dumps([
            {"name": "rent_cad", "description": "Average rent."},
            {"name": "vacancy_rate_pct", "description": "Vacancy rate."},
            {"name": "home_price_cad", "description": "Median home price."},
        ]),
        "notes": "Housing uses city-level metrics and local listing density for map visualization.",
    },
    {
        "formula_key": "employment_score",
        "title": "Employment score",
        "scope": "sector",
        "expression": "100*(0.45*clamp(employment_rate_pct/80,0,1)+0.25*clamp(1-unemployment_rate_pct/12,0,1)+0.30*clamp(participation_rate_pct/80,0,1))",
        "variables_json": json.dumps([
            {"name": "employment_rate_pct", "description": "Employment rate."},
            {"name": "unemployment_rate_pct", "description": "Unemployment rate."},
            {"name": "participation_rate_pct", "description": "Participation rate."},
        ]),
        "notes": "Employment uses city/CMA level metrics.",
    },
    {
        "formula_key": "placemaking_score",
        "title": "Placemaking score",
        "scope": "sector",
        "expression": "100*(0.28*clamp(amenity_count/40,0,1)+0.28*clamp(amenity_diversity/8,0,1)+0.18*clamp(trail_km/20,0,1)+0.10*clamp(walk_links/40,0,1)+0.16*clamp(pop_density/7000,0,1))",
        "variables_json": json.dumps([
            {"name": "amenity_count", "description": "Amenity count."},
            {"name": "amenity_diversity", "description": "Amenity diversity."},
            {"name": "amenity_mix_score", "description": "Count + type tier (red/orange/green)."},
            {"name": "trail_km", "description": "Trail km."},
            {"name": "walk_links", "description": "Walk links count."},
            {"name": "pop_density", "description": "Population density."},
        ]),
        "notes": "Cell score blends this formula (~62%) with amenity_mix_score (~38%).",
    },
]


# ---------- helpers ----------

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def rows_to_dicts(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(r) for r in rows]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def cache_write_json(name: str, data: Any) -> None:
    (CACHE_DIR / name).write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def cache_read_json(name: str, default: Any) -> Any:
    path = CACHE_DIR / name
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return default
    return default


def normalize_observed_at(value: str | None) -> str:
    if not value:
        return now_iso()
    raw = str(value).strip()
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y", "%b %Y", "%B %Y", "%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc).isoformat()
        except Exception:
            pass
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
    except Exception:
        return now_iso()


def log_run(conn: sqlite3.Connection, run_id: int, level: str, message: str) -> None:
    conn.execute("INSERT INTO logs (run_id, created_at, level, message) VALUES (?, ?, ?, ?)", (run_id, now_iso(), level, message))
    conn.commit()


def add_alert(conn: sqlite3.Connection, severity: str, category: str, title: str, message: str, source_url: str | None = None, metric_key: str | None = None, region_scope: str | None = None, region_id: str | None = None, is_demo: int = 0) -> None:
    conn.execute(
        "INSERT INTO alerts (created_at, severity, category, title, message, source_url, metric_key, region_scope, region_id, is_demo) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (now_iso(), severity, category, title, message, source_url, metric_key, region_scope, region_id, is_demo),
    )
    conn.commit()


def add_metric_snapshot(conn: sqlite3.Connection, metric_key: str, value: float, unit: str, source_urls: list[str], metadata: dict[str, Any] | None = None, region_scope: str = "whole", region_id: str | None = None, observed_at: str | None = None) -> None:
    observed = normalize_observed_at(observed_at or (metadata or {}).get("period"))
    recent = conn.execute(
        "SELECT id, value, observed_at FROM metric_snapshots WHERE metric_key = ? AND region_scope = ? AND COALESCE(region_id,'') = COALESCE(?, '') ORDER BY id DESC LIMIT 1",
        (metric_key, region_scope, region_id),
    ).fetchone()
    if recent:
        if str(recent["observed_at"])[:10] == observed[:10] and abs(float(recent["value"]) - float(value)) < 1e-9:
            return
    conn.execute(
        "INSERT INTO metric_snapshots (metric_key, region_scope, region_id, observed_at, value, unit, source_urls_json, metadata_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (metric_key, region_scope, region_id, observed, float(value), unit, json.dumps(sorted(set(source_urls))), json.dumps(metadata or {})),
    )
    conn.commit()


def export_region_metrics_snapshot(conn: sqlite3.Connection) -> None:
    lm = latest_metric_map(conn, "whole", None)
    whole: dict[str, Any] = {}
    for k, row in lm.items():
        whole[k] = {"value": row.get("value"), "unit": row.get("unit"), "observed_at": row.get("observed_at")}
    snap_n = conn.execute("SELECT COUNT(*) FROM metric_snapshots").fetchone()[0]
    cache_write_json(REGION_METRICS_JSON, {"updated_at": now_iso(), "whole": whole, "metric_snapshots_rows": int(snap_n)})


def export_sources_registry(conn: sqlite3.Connection) -> None:
    rows = rows_to_dicts(conn.execute(
        "SELECT sector, name, source_url, access_method, update_frequency, last_checked, status, active, notes FROM sources ORDER BY sector, name",
    ).fetchall())
    cache_write_json(SOURCES_REGISTRY_JSON, {"updated_at": now_iso(), "sources": rows})


def latest_metric_map(conn: sqlite3.Connection, region_scope: str = "whole", region_id: str | None = None) -> dict[str, dict[str, Any]]:
    if region_id is None:
        rows = rows_to_dicts(conn.execute(
            """
            SELECT s.* FROM metric_snapshots s
            JOIN (SELECT metric_key, MAX(id) AS max_id FROM metric_snapshots WHERE region_scope = ? AND region_id IS NULL GROUP BY metric_key) x
            ON s.id = x.max_id
            """,
            (region_scope,),
        ).fetchall())
    else:
        rows = rows_to_dicts(conn.execute(
            """
            SELECT s.* FROM metric_snapshots s
            JOIN (SELECT metric_key, MAX(id) AS max_id FROM metric_snapshots WHERE region_scope = ? AND region_id = ? GROUP BY metric_key) x
            ON s.id = x.max_id
            """,
            (region_scope, region_id),
        ).fetchall())
    return {r["metric_key"]: r for r in rows}


def get_formula(conn: sqlite3.Connection, key: str) -> dict[str, Any]:
    row = conn.execute("SELECT * FROM formulas WHERE formula_key = ?", (key,)).fetchone()
    item = dict(row)
    item["variables"] = json.loads(item.get("variables_json") or "[]")
    return item


def clamp(v: float, low: float, high: float) -> float:
    return max(low, min(high, v))


def normalize_metric(value: float, min_good: float, max_good: float, direction: str) -> float:
    if max_good == min_good:
        return 50.0
    if direction == "higher":
        raw = 100.0 * (value - min_good) / (max_good - min_good)
    else:
        raw = 100.0 * (max_good - value) / (max_good - min_good)
    return round(clamp(raw, 0.0, 100.0), 1)


def amenity_mix_score_and_tier(n_amen: int, n_types: int) -> tuple[float, str]:
    """Map layer score from OSM amenity *types*: under 3 → red; 4–5 → moderate green; 10+ → strong green."""
    nt = max(0, int(n_types))
    na = max(0, int(n_amen))
    count_boost = min(18.0, math.log1p(max(1, na)) * 3)
    print("Ameneties nt", nt)
    if nt < 3:
        sc = clamp(6.0 + nt * 9.0 + count_boost * 0.35, 0.0, 30.0)
        tier = "red"
    elif nt < 4:
        sc = clamp(40.0 + count_boost * 0.4, 0.0, 50.0)
        tier = "orange"
    elif nt <= 5:
        # 4–5 types → moderate green band
        sc = clamp(58.0 + (nt - 4) * 7.0 + count_boost * 0.45, 0.0, 82.0)
        tier = "green"
    elif nt < 10:
        t = (nt - 5) / 4.0
        sc = clamp(72.0 + t * 16.0 + count_boost * 0.35, 0.0, 96.0)
        tier = "green"
    else:
        sc = clamp(88.0 + min(12.0, count_boost * 0.25), 0.0, 100.0)
        tier = "green"
    return round(sc, 1), tier


def safe_eval(expression: str, variables: dict[str, Any]) -> float:
    allowed_funcs = {"clamp": clamp, "min": min, "max": max, "abs": abs, "round": round}
    tree = ast.parse(expression, mode="eval")
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            if not isinstance(node.func, ast.Name) or node.func.id not in allowed_funcs:
                raise ValueError("Unsupported function in formula")
        elif isinstance(node, ast.Name):
            if node.id not in variables and node.id not in allowed_funcs:
                raise ValueError(f"Unsupported variable: {node.id}")
        elif not isinstance(node, (ast.Expression, ast.Call, ast.Name, ast.Load, ast.BinOp, ast.UnaryOp, ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Pow, ast.Mod, ast.USub, ast.UAdd, ast.Constant, ast.Compare, ast.Gt, ast.GtE, ast.Lt, ast.LtE, ast.IfExp, ast.BoolOp, ast.And, ast.Or)):
            raise ValueError("Unsupported formula syntax")
    return float(eval(compile(tree, "<formula>", "eval"), {"__builtins__": {}}, {**allowed_funcs, **variables}))


def fetch_url(url: str, allow_binary: bool = True) -> dict[str, Any]:
    try:
        r = requests.get(url, headers=HTTP_HEADERS, timeout=60)
        ctype = r.headers.get("Content-Type", "")
        return {
            "ok": bool(r.ok),
            "status_code": r.status_code,
            "content_type": ctype,
            "text": r.text if ("text" in ctype or "json" in ctype or "html" in ctype) else "",
            "content": r.content if allow_binary else b"",
        }
    except Exception as exc:
        return {"ok": False, "status_code": None, "content_type": "", "text": str(exc), "content": b""}


def strip_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    return re.sub(r"\s+", " ", soup.get_text(" ", strip=True))


def extract_pdf_text_bytes(data: bytes, max_pages: int = 6) -> str:
    try:
        reader = PdfReader(io.BytesIO(data))
        chunks = []
        for page in reader.pages[:max_pages]:
            chunks.append(page.extract_text() or "")
        return "\n".join(chunks)
    except Exception as exc:
        return f"PDF parse failed: {exc}"


def parse_bbox_arg(arg: str | None) -> tuple[float, float, float, float] | None:
    if not arg:
        return None
    try:
        west, south, east, north = [float(x) for x in arg.split(",")]
    except Exception:
        return None
    if west >= east or south >= north:
        return None
    return west, south, east, north


def bbox_label(bbox: tuple[float, float, float, float] | None) -> str:
    if not bbox:
        return "Whole Waterloo Region"
    west, south, east, north = bbox
    return f"Selected area ({south:.4f}, {west:.4f}) → ({north:.4f}, {east:.4f})"


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = math.sin(d_lat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(d_lon / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def city_boundary_collection() -> dict[str, Any]:
    cached = cache_read_json("city_boundaries.json", {})
    if cached.get("features"):
        return cached
    return fetch_city_boundaries()


# ---------- DB ----------

def init_db() -> None:
    with closing(get_db()) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                sector TEXT NOT NULL,
                source_url TEXT NOT NULL,
                access_method TEXT NOT NULL,
                parser_type TEXT NOT NULL,
                update_frequency TEXT NOT NULL,
                use_llm INTEGER DEFAULT 0,
                active INTEGER DEFAULT 1,
                last_checked TEXT,
                last_success TEXT,
                status TEXT DEFAULT 'seeded',
                notes TEXT,
                preview_text TEXT
            );
            CREATE TABLE IF NOT EXISTS metric_defs (
                metric_key TEXT PRIMARY KEY,
                sector TEXT NOT NULL,
                name TEXT NOT NULL,
                unit TEXT NOT NULL,
                direction TEXT NOT NULL,
                min_good REAL NOT NULL,
                max_good REAL NOT NULL,
                weight REAL NOT NULL,
                description TEXT
            );
            CREATE TABLE IF NOT EXISTS sector_weights (
                sector TEXT PRIMARY KEY,
                weight REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS formulas (
                formula_key TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                scope TEXT NOT NULL,
                expression TEXT NOT NULL,
                variables_json TEXT,
                notes TEXT
            );
            CREATE TABLE IF NOT EXISTS metric_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                metric_key TEXT NOT NULL,
                region_scope TEXT NOT NULL,
                region_id TEXT,
                observed_at TEXT NOT NULL,
                value REAL NOT NULL,
                unit TEXT NOT NULL,
                source_urls_json TEXT,
                metadata_json TEXT
            );
            CREATE TABLE IF NOT EXISTS runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                status TEXT NOT NULL,
                summary_json TEXT
            );
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                level TEXT NOT NULL,
                message TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS llm_extractions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER,
                source_id INTEGER,
                created_at TEXT NOT NULL,
                status TEXT NOT NULL,
                source_name TEXT,
                source_url TEXT,
                extracted_json TEXT,
                raw_excerpt TEXT
            );
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                severity TEXT NOT NULL,
                category TEXT NOT NULL,
                title TEXT NOT NULL,
                message TEXT NOT NULL,
                source_url TEXT,
                metric_key TEXT,
                region_scope TEXT,
                region_id TEXT,
                is_demo INTEGER DEFAULT 0
            );
            """
        )
        if conn.execute("SELECT COUNT(*) FROM sources").fetchone()[0] == 0:
            for src in DEFAULT_SOURCES:
                conn.execute(
                    "INSERT INTO sources (name, sector, source_url, access_method, parser_type, update_frequency, use_llm, notes) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (src["name"], src["sector"], src["source_url"], src["access_method"], src["parser_type"], src["update_frequency"], src["use_llm"], src["notes"]),
                )
        for m in METRIC_DEFS:
            conn.execute(
                "INSERT OR IGNORE INTO metric_defs (metric_key, sector, name, unit, direction, min_good, max_good, weight, description) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (m["metric_key"], m["sector"], m["name"], m["unit"], m["direction"], m["min_good"], m["max_good"], m["weight"], m["description"]),
            )
        if conn.execute("SELECT COUNT(*) FROM formulas").fetchone()[0] == 0:
            for f in DEFAULT_FORMULAS:
                conn.execute("INSERT INTO formulas (formula_key, title, scope, expression, variables_json, notes) VALUES (?, ?, ?, ?, ?, ?)", (f["formula_key"], f["title"], f["scope"], f["expression"], f["variables_json"], f["notes"]))
        if conn.execute("SELECT COUNT(*) FROM sector_weights").fetchone()[0] == 0:
            for sector, weight in DEFAULT_SECTOR_WEIGHTS.items():
                conn.execute("INSERT INTO sector_weights (sector, weight) VALUES (?, ?)", (sector, weight))
        conn.commit()


# ---------- source fetchers ----------

def geocode_address(address: str) -> tuple[float | None, float | None]:
    try:
        r = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": address, "format": "jsonv2", "limit": 1},
            headers={"User-Agent": HTTP_HEADERS["User-Agent"]},
            timeout=40,
        )
        data = r.json()
        if not data:
            return None, None
        return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception:
        return None, None


def fetch_city_boundaries() -> dict[str, Any]:
    out = {"type": "FeatureCollection", "features": []}
    for city in CITY_NAMES:
        try:
            r = requests.get(
                "https://nominatim.openstreetmap.org/search",
                params={"city": city, "state": "Ontario", "country": "Canada", "format": "geojson", "polygon_geojson": 1, "limit": 1},
                headers={"User-Agent": HTTP_HEADERS["User-Agent"]},
                timeout=60,
            )
            data = r.json()
            features = data.get("features") or []
            if features:
                feat = features[0]
                feat["properties"] = {"city": city, **(feat.get("properties") or {})}
                out["features"].append(feat)
                continue
        except Exception:
            pass
        bbox = CITY_FALLBACK_BBOXES[city]
        out["features"].append({
            "type": "Feature",
            "properties": {"city": city, "source": "fallback"},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [bbox["west"], bbox["south"]],
                    [bbox["east"], bbox["south"]],
                    [bbox["east"], bbox["north"]],
                    [bbox["west"], bbox["north"]],
                    [bbox["west"], bbox["south"]],
                ]],
            },
        })
    cache_write_json("city_boundaries.json", out)
    return out


def read_csv_from_zip(zf: zipfile.ZipFile, name: str) -> list[dict[str, str]]:
    with zf.open(name) as fh:
        text = fh.read().decode("utf-8-sig", errors="ignore")
    return list(csv.DictReader(io.StringIO(text)))


def fetch_gtfs_payload() -> dict[str, Any]:
    urls = [
        "https://webapps.regionofwaterloo.ca/api/grt-routes/api/staticfeeds/1",
        "https://www.regionofwaterloo.ca/opendatadownloads/GRT_GTFS.zip",
    ]
    content = None
    used_url = None
    for url in urls:
        try:
            r = requests.get(url, headers=HTTP_HEADERS, timeout=90)
            if r.ok and len(r.content) > 20000:
                content = r.content
                used_url = url
                break
        except Exception:
            pass
    if content is None:
        payload = cache_read_json("transport_gtfs.json", {})
        if payload:
            return payload
        raise RuntimeError("Unable to download GTFS feed.")

    zf = zipfile.ZipFile(io.BytesIO(content))
    routes = {r["route_id"]: r for r in read_csv_from_zip(zf, "routes.txt")}
    trips = read_csv_from_zip(zf, "trips.txt")
    stops = read_csv_from_zip(zf, "stops.txt")
    stop_times = read_csv_from_zip(zf, "stop_times.txt")
    shapes_rows = read_csv_from_zip(zf, "shapes.txt") if "shapes.txt" in zf.namelist() else []

    trip_to_route: dict[str, str] = {}
    for trip in trips:
        if trip.get("trip_id"):
            trip_to_route[trip["trip_id"]] = trip.get("route_id", "")

    stop_trip_count: dict[str, int] = {}
    stop_to_routes: dict[str, set[str]] = {}
    for st in stop_times:
        stop_id = st.get("stop_id")
        trip_id = st.get("trip_id")
        if not stop_id or not trip_id:
            continue
        stop_trip_count[stop_id] = stop_trip_count.get(stop_id, 0) + 1
        rid = trip_to_route.get(trip_id)
        if rid:
            stop_to_routes.setdefault(stop_id, set()).add(rid)

    shapes: dict[str, list[tuple[int, float, float]]] = {}
    for row in shapes_rows:
        try:
            sid = row["shape_id"]
            seq = int(float(row["shape_pt_sequence"]))
            lat = float(row["shape_pt_lat"])
            lon = float(row["shape_pt_lon"])
        except Exception:
            continue
        shapes.setdefault(sid, []).append((seq, lat, lon))

    route_shape: dict[str, str] = {}
    for trip in trips:
        rid = trip.get("route_id")
        sid = trip.get("shape_id")
        if rid and sid and rid not in route_shape:
            route_shape[rid] = sid

    line_features = []
    for rid, sid in route_shape.items():
        coords = [[lon, lat] for seq, lat, lon in sorted(shapes.get(sid, []), key=lambda x: x[0])]
        if len(coords) < 2:
            continue
        route = routes.get(rid, {})
        line_features.append({
            "type": "Feature",
            "properties": {
                "route_id": rid,
                "route_name": route.get("route_short_name") or route.get("route_long_name") or rid,
                "route_long_name": route.get("route_long_name", ""),
            },
            "geometry": {"type": "LineString", "coordinates": coords},
        })

    stop_features = []
    for stop in stops:
        try:
            lat = float(stop.get("stop_lat", "0"))
            lon = float(stop.get("stop_lon", "0"))
        except Exception:
            continue
        if not (REGION_BBOX["south"] <= lat <= REGION_BBOX["north"] and REGION_BBOX["west"] <= lon <= REGION_BBOX["east"]):
            continue
        route_ids = sorted(stop_to_routes.get(stop.get("stop_id", ""), set()))
        stop_features.append({
            "type": "Feature",
            "properties": {
                "stop_id": stop.get("stop_id"),
                "stop_name": stop.get("stop_name", ""),
                "route_count": len(route_ids),
                "trip_count": stop_trip_count.get(stop.get("stop_id", ""), 0),
                "routes": [routes.get(rid, {}).get("route_short_name") or rid for rid in route_ids],
            },
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
        })

    payload = {
        "fetched_at": now_iso(),
        "source_url": used_url,
        "routes": {"type": "FeatureCollection", "features": line_features},
        "stops": {"type": "FeatureCollection", "features": stop_features},
    }
    cache_write_json("transport_gtfs.json", payload)
    return payload


def _parse_traffic_features(data: dict[str, Any], source_label: str | None = None) -> dict[str, Any]:
    features: list[dict[str, Any]] = []
    for feat in (data or {}).get("features", []):
        props = feat.get("properties", {}) or {}
        aadt = props.get("AADT") or props.get("aadt") or props.get("Aadt") or props.get("AnnualAverageDailyTraffic")
        try:
            aadt_val = float(str(aadt).replace(",", "")) if aadt not in (None, "") else None
        except Exception:
            aadt_val = None
        feat.setdefault("properties", {})["AADT"] = aadt_val
        name = (
            props.get("STREET")
            or props.get("STREET_NAME")
            or props.get("ROAD_NAME")
            or props.get("ROAD")
            or props.get("FULL_STREET_NAME")
            or props.get("ROADNAME")
            or "Road segment"
        )
        feat["properties"]["road_name"] = name
        feat["properties"]["source_url"] = source_label
        if feat.get("geometry"):
            features.append(feat)
    return {"fetched_at": now_iso(), "source_url": source_label, "roads": {"type": "FeatureCollection", "features": features}}


def _parse_aadt_rows_csv_text(raw: str) -> dict[str, float]:
    """Parse ArcGIS-style traffic CSV: ROADSEGMENTID, AADT, ..."""
    out: dict[str, float] = {}
    try:
        reader = csv.DictReader(io.StringIO(raw))
        for r in reader:
            if not r:
                continue
            aadt = r.get("AADT") or r.get("aadt")
            rid = r.get("ROADSEGMENTID") or r.get("RoadSegmentId") or r.get("roadsegmentid") or r.get("ROADSEGMENT")
            if not rid or aadt in (None, ""):
                continue
            try:
                out[str(int(float(str(rid).strip())))] = float(str(aadt).replace(",", ""))
            except Exception:
                continue
    except Exception:
        pass
    return out


def _load_aadt_csv_map() -> dict[str, float]:
    """Local CSV (optional) -> ROADSEGMENTID -> AADT."""
    for csv_path in (BASE_DIR / "data" / "Traffic_Volumes.csv", BASE_DIR / "Traffic_Volumes.csv"):
        if csv_path.exists():
            try:
                return _parse_aadt_rows_csv_text(csv_path.read_text(encoding="utf-8", errors="ignore"))
            except Exception:
                pass
    return {}


def _download_traffic_csv_aadt_map() -> dict[str, float]:
    urls = [
        "https://rowopendata-rmw.opendata.arcgis.com/api/download/v1/items/426089c5166c4f8f8f4000acb2fef840/csv?layers=0",
        "https://data.waterloo.ca/api/download/v1/items/426089c5166c4f8f8f4000acb2fef840/csv?layers=0",
    ]
    data_dir = BASE_DIR / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    out_path = data_dir / "Traffic_Volumes.csv"
    for url in urls:
        try:
            r = requests.get(url, headers=HTTP_HEADERS, timeout=120)
            if not r.ok or not (r.text or "").strip():
                continue
            out_path.write_text(r.text, encoding="utf-8")
            m = _parse_aadt_rows_csv_text(r.text)
            if m:
                return m
        except Exception:
            continue
    return {}


def fetch_traffic_volumes() -> dict[str, Any]:
    aadt_map: dict[str, float] = {}
    aadt_map.update(_load_aadt_csv_map())
    aadt_map.update(_download_traffic_csv_aadt_map())

    def _apply_aadt_map(payload: dict[str, Any], source_label: str) -> dict[str, Any]:
        if aadt_map:
            for feat in payload.get("roads", {}).get("features", []):
                props = feat.setdefault("properties", {})
                if props.get("AADT") not in (None, ""):
                    continue
                rid = props.get("ROADSEGMENTID") or props.get("RoadSegmentId") or props.get("roadsegmentid") or props.get("OBJECTID")
                if rid is None:
                    continue
                try:
                    key = str(int(float(rid)))
                except Exception:
                    key = str(rid).strip()
                if key in aadt_map:
                    props["AADT"] = aadt_map[key]
        payload["fetched_at"] = now_iso()
        payload["source_url"] = source_label
        cache_write_json("traffic_volumes.json", payload)
        return payload

    for path in LOCAL_TRAFFIC_GEOJSON_CANDIDATES:
        try:
            if path.exists() and path.stat().st_size > 0:
                data = json.loads(path.read_text(encoding="utf-8"))
                payload = _parse_traffic_features(data, str(path))
                return _apply_aadt_map(payload, str(path))
        except Exception:
            pass
    geo_urls = [
        "https://rowopendata-rmw.opendata.arcgis.com/api/download/v1/items/426089c5166c4f8f8f4000acb2fef840/geojson?layers=0",
        "https://data.waterloo.ca/api/download/v1/items/426089c5166c4f8f8f4000acb2fef840/geojson?layers=0",
    ]
    for url in geo_urls:
        try:
            r = requests.get(url, headers=HTTP_HEADERS, timeout=120)
            if not r.ok:
                continue
            data = r.json()
            payload = _parse_traffic_features(data, url)
            if payload["roads"]["features"]:
                return _apply_aadt_map(payload, url)
        except Exception:
            continue
    payload = {"fetched_at": now_iso(), "source_url": None, "roads": {"type": "FeatureCollection", "features": []}}
    cache_write_json("traffic_volumes.json", payload)
    return payload


def _normalize_hospital_match_name(name: str) -> str:
    s = re.sub(r"[^a-z0-9]+", " ", (name or "").lower())
    for w in ("hospital", "emergency", "department", "regional", "health", "network", "the", "er", "ontario"):
        s = s.replace(w, " ")
    return " ".join(s.split())


def _hospital_name_tokens(name: str) -> set[str]:
    return {t for t in _normalize_hospital_match_name(name).split() if len(t) > 1}


def _er_watch_match_score(fac_name: str, er_name: str, er_slug: str) -> float:
    fn = _normalize_hospital_match_name(fac_name)
    hn = _normalize_hospital_match_name(er_name)
    seq = difflib.SequenceMatcher(None, fn, hn).ratio()
    tf, th = _hospital_name_tokens(fac_name), _hospital_name_tokens(er_name)
    overlap = (len(tf & th) / len(tf | th)) if (tf | th) else 0.0
    slug = (er_slug or "").lower()
    bonus = 0.0
    if "midtown" in fn and "midtown" in slug:
        bonus = 0.35
    if ("queen" in fn or "mary" in fn or "st" in fn) and ("queen" in slug or "wrhn-queen" in slug):
        bonus = max(bonus, 0.35)
    if "cambridge" in fn and "memorial" in fn and "cambridge" in slug:
        bonus = max(bonus, 0.35)
    if "chicopee" in fn and ("chicopee" in slug or "chicopee" in hn):
        bonus = max(bonus, 0.3)
    return min(1.0, max(seq, overlap * 1.15, seq * 0.65 + overlap * 0.35) + bonus)


def _parse_er_watch_wait_hours_html(html: str) -> float | None:
    raw = html or ""
    nd = re.search(r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>', raw, re.I | re.S)
    if nd:
        blob = nd.group(1)
        for pat in (
            r'"totalMinutes"\s*:\s*(\d+)',
            r'"minutesTotal"\s*:\s*(\d+)',
            r'"waitMinutes"\s*:\s*(\d+)',
            r'"hours?"\s*:\s*(\d+)[^}]{0,120}"minutes?"\s*:\s*(\d+)',
        ):
            m = re.search(pat, blob, re.I | re.S)
            if m:
                try:
                    if len(m.groups()) >= 2:
                        return int(m.group(1)) + int(m.group(2)) / 60.0
                    return int(m.group(1)) / 60.0
                except Exception:
                    continue
    patterns = [
        r"(\d+)\s*h\s*(\d+)\s*m",
        r"(\d+)\s*hrs?\s*(\d+)\s*mins?",
        r"Current\s+Wait\s+Time[^0-9]{0,120}(\d+)\s*h\s*(\d+)\s*m",
        r"wait(?:ing)?[^0-9]{0,40}(\d+)\s*h\s*(\d+)\s*m",
        r'"hours?"\s*:\s*(\d+)[^}]{0,80}"minutes?"\s*:\s*(\d+)',
        r"(\d+)\s*hours?\s+(\d+)\s*minutes?",
    ]
    for pat in patterns:
        m = re.search(pat, raw, re.I | re.S)
        if m:
            try:
                return int(m.group(1)) + int(m.group(2)) / 60.0
            except Exception:
                continue
    m = re.search(r"(\d+(?:\.\d+)?)\s*(?:hours?|hrs?)\b(?!\s*\d+\s*mins?)", raw, re.I)
    if m:
        return float(m.group(1))
    m = re.search(r"(\d+)\s*min(?:utes)?(?!\s*\d+\s*h)", raw, re.I)
    if m:
        return int(m.group(1)) / 60.0
    return None


def _er_watch_location_is_kcw(text: str) -> bool:
    t = (text or "").lower()
    if any(x in t for x in ("kitchener", "waterloo", "cambridge", "n2m ", "n2k ", "n2h ", "n2j ", "n2l ", "n2n ", "n1r ", "n1s ", "n3h ", "n1p ")):
        return True
    return False


def scrape_er_watch_hospitals() -> list[dict[str, Any]]:
    """Scrape https://www.er-watch.ca/ hospital pages; KCW seeds always fetched (name/slug matching)."""
    base = "https://www.er-watch.ca"
    out: list[dict[str, Any]] = []
    seed_paths: tuple[str, ...] = (
        "/hospitals/wrhn-midtown",
        "/hospitals/wrhn-queen-s",
        "/hospitals/cambridge-memorial-hospital",
    )
    paths: set[str] = set(seed_paths)
    try:
        main = fetch_url(base + "/")
        if main.get("ok") and main.get("text"):
            for m in re.findall(r'["\'](/hospitals/[a-z0-9-]+)["\']', main["text"], re.I):
                paths.add(m)
    except Exception:
        pass
    seen_url: set[str] = set()

    def ingest(path: str, relax_location: bool) -> None:
        url = base + path if path.startswith("/") else path
        if url in seen_url:
            return
        seen_url.add(url)
        try:
            fr = fetch_url(url)
            if not fr.get("ok"):
                return
            html = fr.get("text") or ""
            plain = strip_text(html)
            if not relax_location and not _er_watch_location_is_kcw(plain) and not _er_watch_location_is_kcw(html):
                return
            wait = _parse_er_watch_wait_hours_html(html)
            title_m = re.search(r"<h1[^>]*>([^<]+)</h1>", html, re.I)
            slug = path.rstrip("/").split("/")[-1] if "/hospitals/" in path else ""
            name = strip_text(title_m.group(1)) if title_m else slug.replace("-", " ").title()
            out.append({"name": name, "slug": slug, "url": url, "wait_hours": wait, "text_sample": plain[:400]})
        except Exception:
            return

    for path in seed_paths:
        ingest(path, relax_location=True)
    for path in sorted(paths):
        if path in seed_paths:
            continue
        ingest(path, relax_location=False)

    return out


FACILITY_ER_SLUG_PREF: dict[str, str] = {
    "WRHN @ Midtown": "wrhn-midtown",
    "Grand River Hospital": "wrhn-midtown",
    "WRHN @ Queen's Blvd.": "wrhn-queen-s",
    "St. Mary's General Hospital": "wrhn-queen-s",
    "Cambridge Memorial Hospital": "cambridge-memorial-hospital",
}


def _assign_er_waits_to_facilities(facilities: list[dict[str, Any]], er_hospitals: list[dict[str, Any]]) -> None:
    if not er_hospitals:
        return
    by_slug = {h.get("slug"): h for h in er_hospitals if h.get("slug")}
    for fac in facilities:
        name = fac.get("name", "") or ""
        pref = FACILITY_ER_SLUG_PREF.get(name)
        picked = None
        if pref and pref in by_slug and by_slug[pref].get("wait_hours") is not None:
            picked = by_slug[pref]
        if picked is None:
            best_r = 0.0
            best_wait = None
            best_label = None
            best_url = None
            best_slug = None
            for h in er_hospitals:
                if h.get("wait_hours") is None:
                    continue
                r = _er_watch_match_score(name, h.get("name", ""), str(h.get("slug") or ""))
                if r > best_r:
                    best_r = r
                    best_wait = float(h["wait_hours"])
                    best_label = h.get("name")
                    best_url = h.get("url")
                    best_slug = h.get("slug")
            thresh = 0.22
            tf, th = _hospital_name_tokens(name), _hospital_name_tokens(best_label or "")
            if tf and th and len(tf & th) >= 1:
                thresh = 0.18
            if best_wait is not None and best_r >= thresh:
                picked = {"name": best_label, "url": best_url, "wait_hours": best_wait, "slug": best_slug}
        if picked is not None:
            fac["wait_hours"] = float(picked["wait_hours"])
            fac["wait_source"] = "https://www.er-watch.ca/"
            fac["wait_matched_er_watch"] = picked.get("name")
            if picked.get("url"):
                fac["er_watch_hospital_url"] = picked["url"]
        elif fac.get("wait_hours") is None:
            fac["wait_hours"] = 5.5
            fac["wait_source"] = "fallback"


def scrape_healthcare_sources() -> dict[str, Any]:
    facilities_seed = [
        ("WRHN @ Midtown", "835 King St W, Kitchener, Ontario"),
        ("WRHN @ Queen's Blvd.", "911 Queen's Blvd, Kitchener, Ontario"),
        ("WRHN @ Chicopee", "3580 King St E, Kitchener, Ontario"),
        ("Cambridge Memorial Hospital", "700 Coronation Blvd, Cambridge, Ontario"),
        ("St. Mary's General Hospital", "911 Queen's Blvd, Kitchener, Ontario"),
        ("Grand River Hospital", "835 King St W, Kitchener, Ontario"),
    ]
    facilities = []
    for name, addr in facilities_seed:
        lat, lon = geocode_address(addr)
        if lat is None or lon is None:
            continue
        facilities.append({"name": name, "address": addr, "lat": lat, "lon": lon, "wait_hours": None, "wait_source": None})

    er_list = scrape_er_watch_hospitals()
    _assign_er_waits_to_facilities(facilities, er_list)
    for fac in facilities:
        if fac.get("wait_hours") is None:
            fac["wait_hours"] = 5.5
            fac["wait_source"] = fac.get("wait_source") or "fallback"

    south, west, north, east = REGION_BBOX["south"], REGION_BBOX["west"], REGION_BBOX["north"], REGION_BBOX["east"]
    q = f"""
    [out:json][timeout:90];
    (
      node["amenity"~"doctors|dentist|clinic|hospital"]({south},{west},{north},{east});
      way["amenity"~"doctors|dentist|clinic|hospital"]({south},{west},{north},{east});
      relation["amenity"~"doctors|dentist|clinic|hospital"]({south},{west},{north},{east});
    );
    out center;
    """
    poi = []
    try:
        r = requests.post("https://overpass-api.de/api/interpreter", data=q.encode("utf-8"), timeout=120)
        data = r.json()
        for el in data.get("elements", []):
            lat = el.get("lat") or el.get("center", {}).get("lat")
            lon = el.get("lon") or el.get("center", {}).get("lon")
            if lat is None or lon is None:
                continue
            tags = el.get("tags", {})
            poi.append({
                "type": "Feature",
                "properties": {"name": tags.get("name", "Unnamed healthcare"), "kind": tags.get("amenity", "other")},
                "geometry": {"type": "Point", "coordinates": [float(lon), float(lat)]},
            })
    except Exception:
        pass
    payload = {"fetched_at": now_iso(), "facilities": facilities, "poi": {"type": "FeatureCollection", "features": poi}}
    cache_write_json("healthcare_points.json", payload)
    cache_write_json("healthcare_waits.json", {"facilities": facilities})
    return payload


def scrape_placemaking_sources() -> dict[str, Any]:
    south, west, north, east = REGION_BBOX["south"], REGION_BBOX["west"], REGION_BBOX["north"], REGION_BBOX["east"]
    amenity_query = f"""
    [out:json][timeout:120];
    (
      node["amenity"~"library|school|community_centre|cafe|bar|restaurant|pub|fast_food|marketplace|bank|post_office|pharmacy|arts_centre|theatre"]({south},{west},{north},{east});
      node["shop"~"supermarket|convenience|hairdresser|bakery|mall"]({south},{west},{north},{east});
      node["leisure"~"park|playground|sports_centre|fitness_centre"]({south},{west},{north},{east});
      way["amenity"~"library|school|community_centre|cafe|bar|restaurant|pub|fast_food|marketplace|bank|post_office|pharmacy|arts_centre|theatre"]({south},{west},{north},{east});
      way["shop"~"supermarket|convenience|hairdresser|bakery|mall"]({south},{west},{north},{east});
      way["leisure"~"park|playground|sports_centre|fitness_centre"]({south},{west},{north},{east});
    );
    out center;
    """
    trail_query = f"""
    [out:json][timeout:120];
    (
      way["highway"~"path|footway|cycleway"]({south},{west},{north},{east});
      way["route"="hiking"]({south},{west},{north},{east});
      way["route"="bicycle"]({south},{west},{north},{east});
    );
    out geom;
    """
    amenities = []
    trails = []
    try:
        r = requests.post("https://overpass-api.de/api/interpreter", data=amenity_query.encode("utf-8"), timeout=120)
        data = r.json()
        for el in data.get("elements", []):
            lat = el.get("lat") or el.get("center", {}).get("lat")
            lon = el.get("lon") or el.get("center", {}).get("lon")
            if lat is None or lon is None:
                continue
            tags = el.get("tags", {})
            kind = tags.get("amenity") or tags.get("shop") or tags.get("leisure") or "other"
            amenities.append({"type": "Feature", "properties": {"name": tags.get("name", "Unnamed amenity"), "kind": kind}, "geometry": {"type": "Point", "coordinates": [float(lon), float(lat)]}})
    except Exception:
        pass
    try:
        r = requests.post("https://overpass-api.de/api/interpreter", data=trail_query.encode("utf-8"), timeout=120)
        data = r.json()
        for el in data.get("elements", []):
            geom = el.get("geometry") or []
            coords = [[pt["lon"], pt["lat"]] for pt in geom if "lon" in pt and "lat" in pt]
            if len(coords) < 2:
                continue
            tags = el.get("tags", {})
            trails.append({"type": "Feature", "properties": {"name": tags.get("name", tags.get("ref", "Trail")), "kind": tags.get("highway") or tags.get("route") or "trail"}, "geometry": {"type": "LineString", "coordinates": coords}})
    except Exception:
        pass
    payload = {"fetched_at": now_iso(), "amenities": {"type": "FeatureCollection", "features": amenities}, "trails": {"type": "FeatureCollection", "features": trails}}
    cache_write_json("placemaking_points.json", payload)
    return payload


def scrape_city_population_density(conn: sqlite3.Connection, run_id: int) -> None:
    pages = {
        "Kitchener": "https://www12.statcan.gc.ca/census-recensement/2021/as-sa/fogs-spg/page.cfm?dguid=2021A00053530013&lang=E&topic=1",
        "Waterloo": "https://www12.statcan.gc.ca/census-recensement/2021/as-sa/fogs-spg/page.cfm?dguid=2021A00053530016&lang=E&topic=1",
        "Cambridge": "https://www12.statcan.gc.ca/census-recensement/2021/as-sa/fogs-spg/page.cfm?dguid=2021A00053530010&lang=E&topic=1",
    }
    boundaries = city_boundary_collection()
    by_city = {f.get("properties", {}).get("city"): shape(f["geometry"]) for f in boundaries.get("features", [])}
    for city, url in pages.items():
        txt = strip_text(fetch_url(url).get("text") or "")
        val = None
        m = re.search(r"Population density[^\n\r]{0,80}?([\d,]+(?:\.\d+)?)", txt, re.I)
        if m:
            try:
                val = float(m.group(1).replace(",", ""))
            except Exception:
                val = None
        if val is None:
            m_pop = re.search(r"Population, 2021[^\n\r]{0,80}?([\d,]+)", txt, re.I)
            if not m_pop:
                m_pop = re.search(r"Total population[^\n\r]{0,80}?([\d,]+)", txt, re.I)
            if m_pop and city in by_city:
                pop = float(m_pop.group(1).replace(",", ""))
                area_km2 = area_sqkm(by_city[city])
                if area_km2 > 0:
                    val = pop / area_km2
        if val is not None and val > 0:
            add_metric_snapshot(conn, "population_density_city", val, "people/km²", [url], {"city": city, "period": "2021-01-01"}, "city", city, observed_at="2021-01-01")
            log_run(conn, run_id, "info", f"Population density captured for {city}: {val:.1f}")


def scrape_employment_sources(conn: sqlite3.Connection, run_id: int) -> None:
    try:
        meta = requests.get("https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/14100459/en", headers=HTTP_HEADERS, timeout=60).json()
        zip_url = meta.get("object")
        if zip_url:
            zr = requests.get(zip_url, headers=HTTP_HEADERS, timeout=120)
            zf = zipfile.ZipFile(io.BytesIO(zr.content))
            csv_name = next((n for n in zf.namelist() if n.lower().endswith(".csv") and "meta" not in n.lower()), None)
            if csv_name:
                reader = csv.DictReader(io.StringIO(zf.read(csv_name).decode("utf-8-sig", errors="ignore")))
                for row in reader:
                    geo = row.get("GEO") or row.get("Geography") or ""
                    if "Kitchener - Cambridge - Waterloo" not in geo:
                        continue
                    char = row.get("Labour force characteristics") or row.get("Labour force characteristics (seasonally adjusted)") or row.get("labour force characteristics") or ""
                    ref = row.get("REF_DATE") or row.get("Reference period") or ""
                    raw = row.get("VALUE") or row.get("Value")
                    try:
                        value = float(str(raw).replace(",", ""))
                    except Exception:
                        continue
                    key = None
                    if "Employment rate" in char:
                        key = "employment_rate_pct"
                    elif "Unemployment rate" in char:
                        key = "unemployment_rate_pct"
                    elif "Participation rate" in char:
                        key = "labour_participation_pct"
                    if not key:
                        continue
                    add_metric_snapshot(conn, key, value, "%", ["https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/14100459/en", zip_url], {"period": ref, "geography": geo}, "whole", None, observed_at=ref)
        log_run(conn, run_id, "info", "Employment history refreshed.")
    except Exception as exc:
        log_run(conn, run_id, "warning", f"Employment history pull failed: {exc}")

    city_pages = {
        "Kitchener": "https://www12.statcan.gc.ca/census-recensement/2021/as-sa/fogs-spg/page.cfm?dguid=2021A00053530013&lang=E&topic=12",
        "Waterloo": "https://www12.statcan.gc.ca/census-recensement/2021/as-sa/fogs-spg/page.cfm?dguid=2021A00053530016&lang=E&topic=12",
        "Cambridge": "https://www12.statcan.gc.ca/census-recensement/2021/as-sa/fogs-spg/page.cfm?dguid=2021A00053530010&lang=E&topic=12",
    }
    pats = {
        "employment_rate_pct": r"Employment rate \(%\)\s*([\d.]+)",
        "unemployment_rate_pct": r"Unemployment rate \(%\)\s*([\d.]+)",
        "labour_participation_pct": r"Participation rate \(%\)\s*([\d.]+)",
    }
    for city, url in city_pages.items():
        txt = strip_text(fetch_url(url).get("text") or "")
        found_any = False
        for key, pat in pats.items():
            m = re.search(pat, txt, re.I)
            if not m:
                continue
            try:
                value = float(m.group(1))
            except Exception:
                continue
            add_metric_snapshot(conn, key, value, "%", [url], {"city": city, "period": "2021-01-01"}, "city", city, observed_at="2021-01-01")
            found_any = True
        if found_any:
            log_run(conn, run_id, "info", f"Employment metrics captured for {city}.")


def scrape_housing_sources(conn: sqlite3.Connection, run_id: int) -> None:
    urls = [
        "https://www03.cmhc-schl.gc.ca/hmip-pimh/en/Profile?a=20&geoId=0850&t=3",
        "https://www03.cmhc-schl.gc.ca/hmip-pimh/en/TableMapChart?id=0850&t=3",
    ]
    combined = " ".join(strip_text(fetch_url(u).get("text") or "") for u in urls)
    # scrape KCW level
    overall_match = re.search(r"Kitchener\s*-\s*Cambridge\s*-\s*Waterloo.*?Vacancy Rate.*?(\d+(?:\.\d+)?)", combined, re.I)
    if overall_match:
        add_metric_snapshot(conn, "housing_vacancy_rate_pct", float(overall_match.group(1)), "%", urls, {"period": "2025-10-01"}, "whole", None, observed_at="2025-10-01")
    # summary lines may contain city names and multiple figures; use weighted fallbacks where needed
    city_patterns = {
        "Waterloo": r"Waterloo\s+(\d+(?:\.\d+)?)\s+(\d[\d,]*(?:\.\d+)?)",
        "Cambridge": r"Cambridge\s+(\d+(?:\.\d+)?)\s+(\d[\d,]*(?:\.\d+)?)",
    }
    for city, pat in city_patterns.items():
        m = re.search(pat, combined, re.I)
        if m:
            vac = float(m.group(1).replace(",", ""))
            rent = float(m.group(2).replace(",", ""))
            add_metric_snapshot(conn, "housing_vacancy_rate_pct", vac, "%", urls, {"city": city, "period": "2025-10-01"}, "city", city, observed_at="2025-10-01")
            add_metric_snapshot(conn, "housing_avg_rent_cad", rent, "CAD", urls, {"city": city, "period": "2025-10-01"}, "city", city, observed_at="2025-10-01")

    # use CMHC headline values if parsing above was weak
    if not conn.execute("SELECT 1 FROM metric_snapshots WHERE metric_key='housing_vacancy_rate_pct' AND region_scope='whole' LIMIT 1").fetchone():
        headline = re.search(r"Vacancy Rate.*?(\d+(?:\.\d+)?)", combined, re.I)
        if headline:
            add_metric_snapshot(conn, "housing_vacancy_rate_pct", float(headline.group(1)), "%", urls, {"period": "2025-10-01"}, "whole", None, observed_at="2025-10-01")
    rent_head = re.search(r"Average Rent.*?2 Bedrooms.*?(\d[\d,]*(?:\.\d+)?)", combined, re.I)
    if rent_head:
        add_metric_snapshot(conn, "housing_avg_rent_cad", float(rent_head.group(1).replace(",", "")), "CAD", urls, {"period": "2025-10-01"}, "whole", None, observed_at="2025-10-01")
    r1 = re.search(r"1\s*Bedroom[^0-9]{0,120}(\d[\d,]*(?:\.\d+)?)", combined, re.I)
    if r1:
        add_metric_snapshot(conn, "housing_avg_rent_1br_cad", float(r1.group(1).replace(",", "")), "CAD", urls, {"period": "2025-10-01", "bedrooms": 1}, "whole", None, observed_at="2025-10-01")
    r3 = re.search(r"3\s*Bedroom[^0-9]{0,120}(\d[\d,]*(?:\.\d+)?)", combined, re.I)
    if r3:
        add_metric_snapshot(conn, "housing_avg_rent_3br_cad", float(r3.group(1).replace(",", "")), "CAD", urls, {"period": "2025-10-01", "bedrooms": 3}, "whole", None, observed_at="2025-10-01")

    # WRAR history for home prices
    archive_url = "https://wrar.ca/category/market-stats/"
    try:
        html = fetch_url(archive_url).get("text") or ""
        links = sorted(set(re.findall(r"https://wrar\.ca/[^\"'\s<>]+", html)))
        seen = 0
        for link in links[:24]:
            txt = strip_text(fetch_url(link).get("text") or "")
            m_price = re.search(r"average sale price(?: for all residential properties)?[^$]{0,80}\$([\d,]+)", txt, re.I)
            m_date = re.search(r"([A-Z][a-z]+ \d{1,2}, \d{4})", txt)
            if not (m_price and m_date):
                continue
            price = float(m_price.group(1).replace(",", ""))
            period = m_date.group(1)
            add_metric_snapshot(conn, "housing_median_price_cad", price, "CAD", [link, archive_url], {"period": period, "proxy": "average_sale_price"}, "whole", None, observed_at=period)
            seen += 1
        if seen:
            latest = conn.execute("SELECT value, observed_at FROM metric_snapshots WHERE metric_key='housing_median_price_cad' AND region_scope='whole' ORDER BY observed_at DESC LIMIT 1").fetchone()
            if latest:
                for city in CITY_NAMES:
                    add_metric_snapshot(conn, "housing_median_price_cad", float(latest["value"]), "CAD", [archive_url], {"city": city, "period": latest["observed_at"][:10], "proxy": "regional_average_sale_price"}, "city", city, observed_at=latest["observed_at"])
        log_run(conn, run_id, "info", f"Housing sources refreshed; {seen} home-price points captured.")
    except Exception as exc:
        log_run(conn, run_id, "warning", f"Housing history pull failed: {exc}")

    create_mock_housing_offers(conn)
    hl = cache_read_json("housing_listings.json", {})
    nfeat = len(hl.get("features") or [])
    if nfeat:
        add_metric_snapshot(
            conn,
            "housing_active_listings_proxy",
            float(nfeat),
            "count",
            ["https://wrar.ca/category/market-stats/", "internal:grid_listings"],
            {"proxy": "count of seeded/demo listing points in region"},
            "whole",
            None,
            observed_at=now_iso(),
        )


def create_mock_housing_offers(conn: sqlite3.Connection) -> None:
    existing = cache_read_json("housing_listings.json", {})
    if existing.get("features"):
        return
    boundaries = city_boundary_collection()
    city_polys = {f["properties"]["city"]: shape(f["geometry"]) for f in boundaries.get("features", [])}
    rng = __import__("random").Random(42)
    latest_city = {city: latest_metric_map(conn, "city", city) for city in CITY_NAMES}
    feats = []
    for city, poly in city_polys.items():
        minx, miny, maxx, maxy = poly.bounds
        avg_rent = float(latest_city.get(city, {}).get("housing_avg_rent_cad", {}).get("value", 1900) or 1900)
        count = {"Kitchener": 45, "Waterloo": 35, "Cambridge": 28}.get(city, 25)
        made = 0
        while made < count:
            x = rng.uniform(minx, maxx)
            y = rng.uniform(miny, maxy)
            p = Point(x, y)
            if not poly.contains(p):
                continue
            asking = round(avg_rent * rng.uniform(0.78, 1.22), 0)
            feats.append({
                "type": "Feature",
                "properties": {"name": f"{city} rental offer {made+1}", "city": city, "asking_rent": asking},
                "geometry": {"type": "Point", "coordinates": [x, y]},
            })
            made += 1
    cache_write_json("housing_listings.json", {"type": "FeatureCollection", "features": feats})


def ensure_nonzero_metrics(conn: sqlite3.Connection, run_id: int) -> None:
    # fill zero/missing city metrics from whole-region or alternate city values
    latest_whole = latest_metric_map(conn, "whole", None)
    for city in CITY_NAMES:
        city_latest = latest_metric_map(conn, "city", city)
        for key in ["housing_avg_rent_cad", "housing_vacancy_rate_pct", "housing_median_price_cad", "employment_rate_pct", "unemployment_rate_pct", "labour_participation_pct", "population_density_city"]:
            val = city_latest.get(key, {}).get("value")
            if val is not None and float(val) > 0:
                continue
            fallback = latest_whole.get(key)
            if fallback and float(fallback.get("value", 0)) > 0:
                meta = json.loads(fallback.get("metadata_json") or "{}")
                meta["proxy"] = "whole-region fallback for zero city score"
                add_metric_snapshot(conn, key, float(fallback["value"]), fallback["unit"], json.loads(fallback.get("source_urls_json") or "[]"), meta, "city", city, observed_at=fallback["observed_at"])
                log_run(conn, run_id, "warning", f"Filled zero/missing {key} for {city} from whole-region source.")
    # whole-region if zero, derive from city average
    latest_whole = latest_metric_map(conn, "whole", None)
    for key in ["housing_avg_rent_cad", "housing_vacancy_rate_pct", "housing_median_price_cad", "employment_rate_pct", "unemployment_rate_pct", "labour_participation_pct"]:
        val = latest_whole.get(key, {}).get("value")
        if val is not None and float(val) > 0:
            continue
        vals = []
        urls = []
        unit = "%"
        for city in CITY_NAMES:
            row = latest_metric_map(conn, "city", city).get(key)
            if row and float(row.get("value", 0)) > 0:
                vals.append(float(row["value"]))
                urls.extend(json.loads(row.get("source_urls_json") or "[]"))
                unit = row["unit"]
        if vals:
            add_metric_snapshot(conn, key, sum(vals)/len(vals), unit, urls or ["derived"], {"proxy": "city average fallback"}, "whole", None)
            log_run(conn, run_id, "warning", f"Filled zero/missing whole-region {key} from city averages.")


WATERLOO_REGION_GEO_RE = re.compile(
    r"waterloo\s*region|regional\s+municipality\s+of\s+waterloo|"
    r"kitchener[-\s]*cambridge[-\s]*waterloo|\bkcw\b|kitchener|cambridge|\bcity\s+of\s+waterloo\b|\bwaterloo\b(?:\s*,\s*on|\s*ontario)?",
    re.I,
)


def _source_url_implies_waterloo_scope(url: str) -> bool:
    u = (url or "").lower()
    return any(
        x in u
        for x in (
            "regionofwaterloo.ca",
            "rowopendata",
            "kitchener.ca",
            "waterloo.ca",
            "cambridge.ca",
            "wrar.ca",
            "wrhn.ca",
            "cmhc-schl.gc.ca",
            "geoId=0850",
            "geographyid=0850",
            "grandriver.ca",
            "grt.ca",
        )
    )


def _metric_geography_is_waterloo_region(item: dict[str, Any], source_url: str) -> bool:
    """Keep LLM metrics that clearly apply to Waterloo Region (or trusted regional sources)."""
    geo = (item.get("geography") or "").strip()
    if geo and WATERLOO_REGION_GEO_RE.search(geo):
        return True
    if not geo and _source_url_implies_waterloo_scope(source_url):
        return True
    return False


def heuristic_extract(source_name: str, source_url: str, excerpt: str) -> dict[str, Any]:
    txt = excerpt.lower()
    metrics: list[dict[str, Any]] = []
    patterns = [
        ("employment_rate_pct", r"employment rate[^\d]{0,40}(\d+(?:\.\d+)?)\s*%", "%"),
        ("unemployment_rate_pct", r"unemployment rate[^\d]{0,40}(\d+(?:\.\d+)?)\s*%", "%"),
        ("labour_participation_pct", r"participation rate[^\d]{0,40}(\d+(?:\.\d+)?)\s*%", "%"),
        ("housing_vacancy_rate_pct", r"vacancy rate[^\d]{0,40}(\d+(?:\.\d+)?)\s*%", "%"),
        ("healthcare_avg_wait_hours", r"(\d+(?:\.\d+)?)\s*hours", "hours"),
        ("population_density_city", r"population density[^\d]{0,40}(\d[\d,]*(?:\.\d+)?)", "people/km²"),
    ]
    for key, pat, unit in patterns:
        m = re.search(pat, txt, re.IGNORECASE)
        if m:
            try:
                value = float(m.group(1).replace(",", ""))
            except Exception:
                continue
            metrics.append({"metric_key": key, "value": value, "unit": unit, "geography": "Waterloo Region", "period": "latest found", "confidence": 0.35})
    currency_matches = re.findall(r"\$\s*([\d,]+(?:\.\d+)?)", excerpt)
    if currency_matches:
        nums = []
        for raw in currency_matches:
            try:
                nums.append(float(raw.replace(",", "")))
            except Exception:
                pass
        if nums:
            metrics.append({"metric_key": "housing_median_price_cad", "value": max(nums), "unit": "CAD", "geography": "Waterloo Region", "period": "latest found", "confidence": 0.15})
            metrics.append({"metric_key": "housing_avg_rent_cad", "value": min(nums), "unit": "CAD", "geography": "Waterloo Region", "period": "latest found", "confidence": 0.1})
    return {"summary": excerpt[:1000], "metrics": metrics, "caveats": ["Heuristic extraction only; verify before publishing."]}


def _parse_llm_json_text(text: str) -> dict[str, Any]:
    t = (text or "").strip()
    if t.startswith("```"):
        t = re.sub(r"^```(?:json)?\s*", "", t, flags=re.I)
        t = re.sub(r"\s*```\s*$", "", t)
    return json.loads(t)


def gemini_extract_json(
    source_name: str,
    source_url: str,
    excerpt: str,
    mime_type: str | None = None,
    content_bytes: bytes | None = None,
) -> tuple[str, dict[str, Any]]:
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    model = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")
    if not api_key or (not (excerpt or "").strip() and not content_bytes):
        return "heuristic", heuristic_extract(source_name, source_url, excerpt or "")

    prompt = f"""
You extract facts for the Waterloo Region (Ontario) community scorecard only.
Return strict JSON with keys: summary, metrics, caveats.

metrics must be an array of objects with keys:
metric_key, value, unit, geography, period, confidence (0-1).

Allowed metric_key values only:
housing_avg_rent_cad
housing_vacancy_rate_pct
housing_median_price_cad
employment_rate_pct
unemployment_rate_pct
labour_participation_pct
healthcare_avg_wait_hours
population_density_city

CRITICAL: Include ONLY values that explicitly refer to Waterloo Region, the Regional Municipality of Waterloo,
the Kitchener-Cambridge-Waterloo census metropolitan area, or the cities Kitchener, Waterloo, or Cambridge.
Do not return figures for other provinces, Canada totals, Toronto, or other CMAs unless the text clearly states
they are the same as the Waterloo Region / KCW value. If no Waterloo-scoped number is present, return metrics: [].
For every metric, set geography to a short string that proves the scope (e.g. "Kitchener - Cambridge - Waterloo CMA").

Do not invent numbers. Use only values supported by the excerpt.

Source name: {source_name}
Source URL: {source_url}
""".strip()

    parts: list[dict[str, Any]] = [{"text": prompt + "\n\nText:\n" + (excerpt or "")[:14000]}]
    if mime_type and content_bytes and "pdf" in mime_type.lower() and len(content_bytes) <= 18_000_000:
        parts.append({"inlineData": {"mimeType": mime_type, "data": base64.b64encode(content_bytes).decode("ascii")}})
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
    payload_req = {
        "contents": [{"parts": parts}],
        "generationConfig": {"temperature": 0, "responseMimeType": "application/json"},
    }
    try:
        r = requests.post(url, headers={"Content-Type": "application/json"}, json=payload_req, timeout=120)
        r.raise_for_status()
        data = r.json()
        text = data["candidates"][0]["content"]["parts"][0]["text"]
        return "gemini", _parse_llm_json_text(text)
    except Exception as exc:
        out = heuristic_extract(source_name, source_url, excerpt or "")
        out.setdefault("caveats", []).append(f"Gemini extraction failed: {exc}")
        return "heuristic", out


def refresh_source_previews(conn: sqlite3.Connection, run_id: int) -> None:
    sources = rows_to_dicts(conn.execute("SELECT * FROM sources WHERE active = 1").fetchall())
    for src in sources:
        fetched = fetch_url(src["source_url"])
        mime_type = (fetched.get("content_type") or "") or ""
        text_body = fetched.get("text") or ""
        content_bin = fetched.get("content") or b""

        if src["access_method"] == "pdf" or ("pdf" in mime_type.lower() and content_bin):
            excerpt = extract_pdf_text_bytes(content_bin) or strip_text(text_body)
            preview = (excerpt or "")[:500]
        elif text_body and "<html" in text_body[:300].lower():
            excerpt = strip_text(text_body)
            preview = excerpt[:500]
        else:
            excerpt = text_body or ""
            preview = strip_text(excerpt)[:500] if excerpt else ""

        if int(src.get("use_llm") or 0):
            ex_full = excerpt if len(excerpt) > 200 else (strip_text(text_body) if text_body else excerpt)
            if src["access_method"] == "pdf" or ("pdf" in mime_type.lower() and content_bin):
                ex_full = extract_pdf_text_bytes(content_bin) or ex_full
            elif text_body and "<html" in text_body[:300].lower():
                ex_full = strip_text(text_body)
            status, payload = gemini_extract_json(src["name"], src["source_url"], ex_full, mime_type=mime_type or None, content_bytes=content_bin if content_bin else None)
            raw_ex = (ex_full or preview or "")[:12000]
            before = len(payload.get("metrics") or [])
            payload["metrics"] = [m for m in (payload.get("metrics") or []) if _metric_geography_is_waterloo_region(m, src["source_url"])]
            after = len(payload["metrics"])
            if before > after:
                payload.setdefault("caveats", []).append(f"Dropped {before - after} metric(s) not scoped to Waterloo Region.")
            conn.execute(
                "UPDATE sources SET last_checked = ?, last_success = ?, status = ?, preview_text = ? WHERE id = ?",
                (now_iso(), now_iso() if fetched.get("ok") else None, status, preview, src["id"]),
            )
            conn.execute(
                "INSERT INTO llm_extractions (run_id, source_id, created_at, status, source_name, source_url, extracted_json, raw_excerpt) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (run_id, src["id"], now_iso(), status, src["name"], src["source_url"], json.dumps(payload), raw_ex[:12000]),
            )
            for item in payload.get("metrics") or []:
                key = item.get("metric_key")
                if not key:
                    continue
                try:
                    value = float(item.get("value"))
                except Exception:
                    continue
                metric_row = conn.execute("SELECT unit FROM metric_defs WHERE metric_key = ?", (key,)).fetchone()
                if not metric_row:
                    continue
                add_metric_snapshot(
                    conn,
                    key,
                    value,
                    item.get("unit") or metric_row[0],
                    [src["source_url"]],
                    {"geography": item.get("geography"), "period": item.get("period"), "confidence": item.get("confidence"), "llm": status},
                    region_scope="whole",
                )
        else:
            conn.execute(
                "UPDATE sources SET last_checked = ?, last_success = ?, status = ?, preview_text = ? WHERE id = ?",
                (now_iso(), now_iso(), "ok" if fetched.get("ok") else "warning", preview, src["id"]),
            )
            conn.execute(
                "INSERT INTO llm_extractions (run_id, source_id, created_at, status, source_name, source_url, extracted_json, raw_excerpt) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (run_id, src["id"], now_iso(), "preview", src["name"], src["source_url"], json.dumps({"summary": preview[:200], "metrics": []}), preview[:1200]),
            )
    conn.commit()


# ---------- geometry and grid ----------

def area_sqkm(geom) -> float:
    # rough lat/lon to km² conversion centered on region
    if geom.is_empty:
        return 0.0
    lat_factor = 111.32
    lon_factor = 111.32 * math.cos(math.radians(43.45))
    return abs(geom.area) * lat_factor * lon_factor


def geojson_feature_to_shape(feature: dict[str, Any]):
    try:
        return shape(feature["geometry"])
    except Exception:
        return None


def color_for_score(score: float) -> str:
    s = clamp(score, 0, 100) / 100.0
    # red -> amber -> green
    if s <= 0.5:
        t = s / 0.5
        r1, g1, b1 = (220, 38, 38)
        r2, g2, b2 = (245, 158, 11)
    else:
        t = (s - 0.5) / 0.5
        r1, g1, b1 = (245, 158, 11)
        r2, g2, b2 = (22, 163, 74)
    r = round(r1 + (r2 - r1) * t)
    g = round(g1 + (g2 - g1) * t)
    b = round(b1 + (b2 - b1) * t)
    return f"rgb({r},{g},{b})"


def build_city_grid(boundaries_fc: dict[str, Any], step: float = DEFAULT_GRID_STEP) -> list[dict[str, Any]]:
    """Create a clipped grid: cells intersecting each city polygon are clipped to that polygon, and outside cells are removed."""
    cells: list[dict[str, Any]] = []
    for feature in boundaries_fc.get("features", []):
        city = feature.get("properties", {}).get("city")
        if not city:
            continue
        city_geom = geojson_feature_to_shape(feature)
        if city_geom is None or city_geom.is_empty:
            continue
        minx, miny, maxx, maxy = city_geom.bounds
        lat = miny
        rid = 0
        while lat < maxy:
            lon = minx
            cid = 0
            while lon < maxx:
                square = box(lon, lat, min(lon + step, maxx), min(lat + step, maxy))
                if not square.intersects(city_geom):
                    lon += step
                    cid += 1
                    continue
                clipped = square.intersection(city_geom)
                if clipped.is_empty:
                    lon += step
                    cid += 1
                    continue
                # drop tiny slivers
                if area_sqkm(clipped) < 0.10:
                    lon += step
                    cid += 1
                    continue
                centroid = clipped.centroid
                cells.append({
                    "id": f"{city[:3].lower()}_{rid}_{cid}",
                    "city": city,
                    "geometry": mapping(clipped),
                    "shape": clipped,
                    "centroid": (centroid.y, centroid.x),
                    "bbox": list(clipped.bounds),
                    "area_sqkm": area_sqkm(clipped),
                })
                lon += step
                cid += 1
            lat += step
            rid += 1
    return cells


def point_feature(feature: dict[str, Any]) -> Point | None:
    try:
        x, y = feature["geometry"]["coordinates"][:2]
        return Point(float(x), float(y))
    except Exception:
        return None


def line_feature_shape(feature: dict[str, Any]):
    try:
        return shape(feature["geometry"])
    except Exception:
        return None


def build_cell_cache(conn: sqlite3.Connection) -> dict[str, Any]:
    boundaries_fc = city_boundary_collection()
    cache_sig = {
        "boundaries": cache_read_json("city_boundaries.json", {}).get("features", []).__len__(),
        "gtfs_at": cache_read_json("transport_gtfs.json", {}).get("fetched_at"),
        "health_at": cache_read_json("healthcare_points.json", {}).get("fetched_at"),
        "place_at": cache_read_json("placemaking_points.json", {}).get("fetched_at"),
        "traffic_at": cache_read_json("traffic_volumes.json", {}).get("fetched_at"),
        "housing_at": cache_read_json("housing_listings.json", {}).get("type"),
    }
    cached = cache_read_json("grid_cells_v10.json", {})
    if cached.get("signature") == cache_sig and cached.get("cells"):
        return cached

    gtfs = cache_read_json("transport_gtfs.json", {})
    healthcare = cache_read_json("healthcare_points.json", {})
    placemaking = cache_read_json("placemaking_points.json", {})
    traffic = cache_read_json("traffic_volumes.json", {})
    housing_offers = cache_read_json("housing_listings.json", {"features": []})

    city_latest = {city: latest_metric_map(conn, "city", city) for city in CITY_NAMES}
    whole_latest = latest_metric_map(conn, "whole", None)
    formulas = {k: get_formula(conn, k) for k in ["transport_score", "healthcare_score", "housing_score", "employment_score", "placemaking_score"]}

    cells = build_city_grid(boundaries_fc, DEFAULT_GRID_STEP)

    stop_shapes = []
    for feat in gtfs.get("stops", {}).get("features", []):
        pt = point_feature(feat)
        if pt is not None:
            stop_shapes.append((pt, feat))
    route_shapes = []
    for feat in gtfs.get("routes", {}).get("features", []):
        geom = line_feature_shape(feat)
        if geom is not None:
            route_shapes.append((geom, feat))
    traffic_shapes = []
    for feat in traffic.get("roads", {}).get("features", []):
        geom = line_feature_shape(feat)
        if geom is not None:
            traffic_shapes.append((geom, feat))
    hospital_points = []
    for fac in healthcare.get("facilities", []):
        p = Point(float(fac["lon"]), float(fac["lat"]))
        hospital_points.append((p, fac))
    healthcare_points = []
    for feat in healthcare.get("poi", {}).get("features", []):
        pt = point_feature(feat)
        if pt is not None:
            healthcare_points.append((pt, feat))
    amenity_points = []
    for feat in placemaking.get("amenities", {}).get("features", []):
        pt = point_feature(feat)
        if pt is not None:
            amenity_points.append((pt, feat))
    trail_shapes = []
    for feat in placemaking.get("trails", {}).get("features", []):
        geom = line_feature_shape(feat)
        if geom is not None:
            trail_shapes.append((geom, feat))
    listing_points = []
    for feat in housing_offers.get("features", []):
        pt = point_feature(feat)
        if pt is not None:
            listing_points.append((pt, feat))

    for cell in cells:
        geom = cell["shape"]
        centroid_pt = Point(cell["centroid"][1], cell["centroid"][0])
        city = cell["city"]
        city_metrics = city_latest.get(city, {})
        pop_density = float(city_metrics.get("population_density_city", {}).get("value", whole_latest.get("population_density_city", {}).get("value", 2500)) or 2500)

        # buffers in degrees (rough)
        near_buf = centroid_pt.buffer(0.010)   # ~1 km
        far_buf = centroid_pt.buffer(0.020)    # ~2 km

        # transport
        nearby_stops = [feat for pt, feat in stop_shapes if pt.within(far_buf)]
        routes_near = set()
        min_stop_km = None
        stop_trip_sum = 0.0
        for pt, feat in stop_shapes:
            dist = haversine_km(cell["centroid"][0], cell["centroid"][1], pt.y, pt.x)
            if min_stop_km is None or dist < min_stop_km:
                min_stop_km = dist
            if pt.within(far_buf):
                routes_near.update(feat.get("properties", {}).get("routes", []))
                stop_trip_sum += float(feat.get("properties", {}).get("trip_count", 0) or 0)
        roads_in = []
        for road_geom, feat in traffic_shapes:
            if road_geom.intersects(geom):
                roads_in.append(float(feat.get("properties", {}).get("AADT") or 0))
        avg_aadt = sum(roads_in) / len(roads_in) if roads_in else 0.0
        transport_vars = {
            "stop_density": float(len(nearby_stops)),
            "route_density": float(len(routes_near)),
            "service_density": float(stop_trip_sum),
            "avg_stop_distance_m": (min_stop_km or 1.2) * 1000.0,
            "avg_aadt": avg_aadt,
            "pop_density": pop_density,
        }
        transport_score = safe_eval(formulas["transport_score"]["expression"], transport_vars)

        # healthcare
        min_hosp_km = None
        wait_h = None
        for hp, fac in hospital_points:
            d = haversine_km(cell["centroid"][0], cell["centroid"][1], hp.y, hp.x)
            if min_hosp_km is None or d < min_hosp_km:
                min_hosp_km = d
                wait_h = float(fac.get("wait_hours") or 5.5)
        docs = dens = clinics = 0
        for pt, feat in healthcare_points:
            if pt.within(far_buf):
                kind = (feat.get("properties", {}).get("kind") or "").lower()
                if kind == "doctors":
                    docs += 1
                elif kind == "dentist":
                    dens += 1
                elif kind == "clinic":
                    clinics += 1
        healthcare_vars = {
            "avg_hospital_distance_km": min_hosp_km or 12.0,
            "avg_wait_hours": wait_h or 5.5,
            "doctors_count": float(docs),
            "dentists_count": float(dens),
            "clinics_count": float(clinics),
        }
        healthcare_score = safe_eval(formulas["healthcare_score"]["expression"], healthcare_vars)

        # housing
        local_offers = [feat for pt, feat in listing_points if pt.within(far_buf)]
        offer_rents = [float(f.get("properties", {}).get("asking_rent") or 0) for f in local_offers if float(f.get("properties", {}).get("asking_rent") or 0) > 0]
        rent_cad = float(city_metrics.get("housing_avg_rent_cad", {}).get("value", whole_latest.get("housing_avg_rent_cad", {}).get("value", 1900)) or 1900)
        if offer_rents:
            local_rent = sum(offer_rents) / len(offer_rents)
        else:
            local_rent = rent_cad
        vacancy = float(city_metrics.get("housing_vacancy_rate_pct", {}).get("value", whole_latest.get("housing_vacancy_rate_pct", {}).get("value", 3.0)) or 3.0)
        home_price = float(city_metrics.get("housing_median_price_cad", {}).get("value", whole_latest.get("housing_median_price_cad", {}).get("value", 700000)) or 700000)
        housing_vars = {"rent_cad": local_rent, "vacancy_rate_pct": vacancy, "home_price_cad": home_price}
        housing_score = safe_eval(formulas["housing_score"]["expression"], housing_vars)

        # employment
        emp = float(city_metrics.get("employment_rate_pct", {}).get("value", whole_latest.get("employment_rate_pct", {}).get("value", 62)) or 62)
        unemp = float(city_metrics.get("unemployment_rate_pct", {}).get("value", whole_latest.get("unemployment_rate_pct", {}).get("value", 6)) or 6)
        part = float(city_metrics.get("labour_participation_pct", {}).get("value", whole_latest.get("labour_participation_pct", {}).get("value", 66)) or 66)
        employment_vars = {"employment_rate_pct": emp, "unemployment_rate_pct": unemp, "participation_rate_pct": part}
        employment_score = safe_eval(formulas["employment_score"]["expression"], employment_vars)

        # placemaking
        amens = [feat for pt, feat in amenity_points if pt.within(far_buf)]
        amenity_types = sorted({str(feat.get("properties", {}).get("kind") or "other").lower() for feat in amens})
        trail_km = 0.0
        trail_links = 0
        for trail_geom, _feat in trail_shapes:
            if trail_geom.intersects(geom):
                trail_links += 1
                trail_km += trail_geom.intersection(geom).length * 111.32
        mix_score, mix_tier = amenity_mix_score_and_tier(len(amens), len(amenity_types))
        placemaking_vars = {
            "amenity_count": float(len(amens)),
            "amenity_diversity": float(len(amenity_types)),
            "trail_km": trail_km,
            "walk_links": float(trail_links),
            "pop_density": pop_density,
            "amenity_mix_score": mix_score,
        }
        try:
            base_pm = safe_eval(formulas["placemaking_score"]["expression"], placemaking_vars)
        except Exception:
            base_pm = 50.0
        placemaking_score = round(clamp(0.62 * base_pm + 0.38 * mix_score, 0.0, 100.0), 1)

        cell["transportation"] = {
            **transport_vars,
            "transit_score": transport_score,
            "traffic_score": normalize_metric(avg_aadt or 0.0, 2000.0, 50000.0, "lower"),
            "score": transport_score,
        }
        cell["healthcare"] = {**healthcare_vars, "score": healthcare_score}
        cell["housing"] = {
            "offer_count": float(len(local_offers)),
            "local_rent_cad": local_rent,
            "vacancy_rate_pct": vacancy,
            "water_capacity_pct": 100.0,
            "home_price_cad": home_price,
            "score": housing_score,
        }
        cell["employment"] = {**employment_vars, "score": employment_score}
        cell["placemaking"] = {
            **placemaking_vars,
            "amenity_types": amenity_types,
            "amenity_mix_tier": mix_tier,
            "score": placemaking_score,
        }

    payload = {
        "signature": cache_sig,
        "cells": [
            {
                "id": c["id"],
                "city": c["city"],
                "geometry": c["geometry"],
                "centroid": c["centroid"],
                "bbox": c["bbox"],
                "area_sqkm": c["area_sqkm"],
                "transportation": c["transportation"],
                "healthcare": c["healthcare"],
                "housing": c["housing"],
                "employment": c["employment"],
                "placemaking": c["placemaking"],
            }
            for c in cells
        ],
    }
    cache_write_json("grid_cells_v10.json", payload)
    return payload


def selection_polygon(bbox: tuple[float, float, float, float] | None):
    if not bbox:
        return None
    west, south, east, north = bbox
    return box(west, south, east, north)


def cell_matches_selection(cell: dict[str, Any], bbox: tuple[float, float, float, float] | None) -> bool:
    if not bbox:
        return True
    sel = selection_polygon(bbox)
    geom = shape(cell["geometry"])
    return geom.intersects(sel)


def aggregate_cells(cells: list[dict[str, Any]]) -> dict[str, Any]:
    if not cells:
        return {}
    def avg(values):
        vals = [float(v) for v in values if v is not None]
        return sum(vals) / len(vals) if vals else 0.0
    return {
        "transportation": {
            "transport_stop_density": avg(c["transportation"]["stop_density"] for c in cells),
            "transport_avg_stop_distance_m": avg(c["transportation"]["avg_stop_distance_m"] for c in cells),
            "transport_route_density": avg(c["transportation"]["route_density"] for c in cells),
            "transport_service_density": avg(c["transportation"]["service_density"] for c in cells),
            "transport_avg_aadt": avg(c["transportation"]["avg_aadt"] for c in cells),
            "score": avg(c["transportation"]["score"] for c in cells),
        },
        "healthcare": {
            "healthcare_avg_wait_hours": avg(c["healthcare"]["avg_wait_hours"] for c in cells),
            "healthcare_avg_hospital_distance_km": avg(c["healthcare"]["avg_hospital_distance_km"] for c in cells),
            "healthcare_doctors_count": avg(c["healthcare"]["doctors_count"] for c in cells),
            "healthcare_dentists_count": avg(c["healthcare"]["dentists_count"] for c in cells),
            "healthcare_clinics_count": avg(c["healthcare"]["clinics_count"] for c in cells),
            "score": avg(c["healthcare"]["score"] for c in cells),
        },
        "housing": {
            "offer_count": avg(c["housing"]["offer_count"] for c in cells),
            "housing_avg_rent_cad": avg(c["housing"]["local_rent_cad"] for c in cells),
            "housing_vacancy_rate_pct": avg(c["housing"]["vacancy_rate_pct"] for c in cells),
            "housing_median_price_cad": avg(c["housing"]["home_price_cad"] for c in cells),
            "listing_points_proxy": sum(float(c["housing"]["offer_count"] or 0) for c in cells) / max(len(cells), 1),
            "score": avg(c["housing"]["score"] for c in cells),
        },
        "employment": {
            "employment_rate_pct": avg(c["employment"]["employment_rate_pct"] for c in cells),
            "unemployment_rate_pct": avg(c["employment"]["unemployment_rate_pct"] for c in cells),
            "labour_participation_pct": avg(c["employment"]["participation_rate_pct"] for c in cells),
            "score": avg(c["employment"]["score"] for c in cells),
        },
        "placemaking": {
            "placemaking_amenity_count": avg(c["placemaking"]["amenity_count"] for c in cells),
            "placemaking_amenity_diversity": avg(c["placemaking"]["amenity_diversity"] for c in cells),
            "placemaking_amenity_mix_score": avg(c["placemaking"].get("amenity_mix_score", 0) for c in cells),
            "placemaking_trail_km": avg(c["placemaking"]["trail_km"] for c in cells),
            "placemaking_walk_links": avg(c["placemaking"]["walk_links"] for c in cells),
            "score": avg(c["placemaking"]["score"] for c in cells),
        },
    }


def city_score_cards(cell_cache: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    for city in CITY_NAMES:
        city_cells = [c for c in cell_cache.get("cells", []) if c["city"] == city]
        agg = aggregate_cells(city_cells)
        out.append({
            "city": city,
            "Housing": round(agg.get("housing", {}).get("score", 0), 1),
            "Transportation": round(agg.get("transportation", {}).get("score", 0), 1),
            "Healthcare": round(agg.get("healthcare", {}).get("score", 0), 1),
            "Employment": round(agg.get("employment", {}).get("score", 0), 1),
            "Placemaking": round(agg.get("placemaking", {}).get("score", 0), 1),
        })
    return out


def metric_meta(metric_key: str) -> dict[str, Any]:
    for m in METRIC_DEFS:
        if m["metric_key"] == metric_key:
            return m
    return {"name": metric_key, "unit": "", "direction": "higher", "min_good": 0, "max_good": 100, "sector": ""}


def weight_meta(conn: sqlite3.Connection) -> dict[str, Any]:
    sectors = rows_to_dicts(conn.execute("SELECT * FROM sector_weights ORDER BY sector").fetchall())
    metrics = rows_to_dicts(conn.execute("SELECT metric_key, sector, name, weight FROM metric_defs ORDER BY sector, name").fetchall())
    return {"sectors": sectors, "metrics": metrics}


def build_summary(conn: sqlite3.Connection, bbox: tuple[float, float, float, float] | None) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    cell_cache = build_cell_cache(conn)
    chosen = [c for c in cell_cache.get("cells", []) if cell_matches_selection(c, bbox)]
    if not chosen:
        chosen = cell_cache.get("cells", [])
    agg = aggregate_cells(chosen)
    sector_weights = {r["sector"]: float(r["weight"]) for r in rows_to_dicts(conn.execute("SELECT * FROM sector_weights").fetchall())}
    metric_defs = {m["metric_key"]: m for m in METRIC_DEFS}

    metrics = []
    def add_metric_item(key, value, sources=None):
        meta = metric_defs[key]
        metrics.append({
            "metric_key": key,
            "sector": meta["sector"],
            "name": meta["name"],
            "unit": meta["unit"],
            "value": value,
            "score": normalize_metric(value, meta["min_good"], meta["max_good"], meta["direction"]),
            "sources": sources or [],
        })

    cmhc_urls = ["https://www03.cmhc-schl.gc.ca/hmip-pimh/en/Profile?a=20&geoId=0850&t=3", "https://www03.cmhc-schl.gc.ca/hmip-pimh/en/TableMapChart?id=0850&t=3"]
    add_metric_item("housing_avg_rent_cad", agg["housing"]["housing_avg_rent_cad"], cmhc_urls)
    add_metric_item("housing_vacancy_rate_pct", agg["housing"]["housing_vacancy_rate_pct"], cmhc_urls)
    add_metric_item("housing_median_price_cad", agg["housing"]["housing_median_price_cad"], ["https://wrar.ca/category/market-stats/"])
    add_metric_item("housing_active_listings_proxy", agg["housing"]["listing_points_proxy"], ["https://wrar.ca/category/market-stats/", "grid demo listings"])
    latest_whole_row = latest_metric_map(conn, "whole", None)
    for mk in ("housing_avg_rent_1br_cad", "housing_avg_rent_3br_cad"):
        snap = latest_whole_row.get(mk)
        if snap and snap.get("value") is not None:
            add_metric_item(mk, float(snap["value"]), cmhc_urls)
    add_metric_item("transport_stop_density", agg["transportation"]["transport_stop_density"], ["https://www.grt.ca/en/about-grt/open-data.aspx"])
    add_metric_item("transport_avg_stop_distance_m", agg["transportation"]["transport_avg_stop_distance_m"], ["https://www.grt.ca/en/about-grt/open-data.aspx"])
    add_metric_item("transport_route_density", agg["transportation"]["transport_route_density"], ["https://www.grt.ca/en/about-grt/open-data.aspx"])
    add_metric_item("transport_service_density", agg["transportation"]["transport_service_density"], ["https://www.grt.ca/en/about-grt/open-data.aspx"])
    add_metric_item("transport_avg_aadt", agg["transportation"]["transport_avg_aadt"], ["https://rowopendata-rmw.opendata.arcgis.com/items/426089c5166c4f8f8f4000acb2fef840"])
    add_metric_item("healthcare_avg_wait_hours", agg["healthcare"]["healthcare_avg_wait_hours"], ["https://www.er-watch.ca/", "https://www.ontario.ca/page/time-spent-emergency-department"])
    add_metric_item("healthcare_avg_hospital_distance_km", agg["healthcare"]["healthcare_avg_hospital_distance_km"], ["https://www.regionofwaterloo.ca/en/living-here/hospitals.aspx"])
    add_metric_item("healthcare_doctors_count", agg["healthcare"]["healthcare_doctors_count"], ["https://overpass-api.de/api/interpreter"])
    add_metric_item("healthcare_dentists_count", agg["healthcare"]["healthcare_dentists_count"], ["https://overpass-api.de/api/interpreter"])
    add_metric_item("healthcare_clinics_count", agg["healthcare"]["healthcare_clinics_count"], ["https://overpass-api.de/api/interpreter"])
    add_metric_item("unemployment_rate_pct", agg["employment"]["unemployment_rate_pct"], ["https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1410045901"])
    add_metric_item("employment_rate_pct", agg["employment"]["employment_rate_pct"], ["https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1410045901"])
    add_metric_item("labour_participation_pct", agg["employment"]["labour_participation_pct"], ["https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1410045901"])
    add_metric_item("placemaking_amenity_count", agg["placemaking"]["placemaking_amenity_count"], ["https://overpass-api.de/api/interpreter"])
    add_metric_item("placemaking_amenity_diversity", agg["placemaking"]["placemaking_amenity_diversity"], ["https://overpass-api.de/api/interpreter"])
    add_metric_item("placemaking_amenity_mix_score", agg["placemaking"]["placemaking_amenity_mix_score"], ["https://overpass-api.de/api/interpreter"])
    add_metric_item("placemaking_trail_km", agg["placemaking"]["placemaking_trail_km"], ["https://overpass-api.de/api/interpreter"])
    add_metric_item("placemaking_walk_links", agg["placemaking"]["placemaking_walk_links"], ["https://overpass-api.de/api/interpreter"])

    sector_details = [
        {"sector": "Housing", "score": round(agg["housing"]["score"], 1), "submetrics": [m for m in metrics if m["sector"] == "Housing"]},
        {"sector": "Transportation", "score": round(agg["transportation"]["score"], 1), "submetrics": [m for m in metrics if m["sector"] == "Transportation"]},
        {"sector": "Healthcare", "score": round(agg["healthcare"]["score"], 1), "submetrics": [m for m in metrics if m["sector"] == "Healthcare"]},
        {"sector": "Employment", "score": round(agg["employment"]["score"], 1), "submetrics": [m for m in metrics if m["sector"] == "Employment"]},
        {"sector": "Placemaking", "score": round(agg["placemaking"]["score"], 1), "submetrics": [m for m in metrics if m["sector"] == "Placemaking"]},
    ]
    total_weight = sum(sector_weights.values()) or 1.0
    overall_score = sum(sector_weights.get(sec["sector"], 1.0) * sec["score"] for sec in sector_details) / total_weight
    summary = {
        "selection_label": bbox_label(bbox),
        "overall_score": round(overall_score, 1),
        "sector_details": sector_details,
    }
    return summary, metrics


def overlay_payload_for_layer(conn: sqlite3.Connection, layer: str) -> dict[str, Any]:
    gtfs = cache_read_json("transport_gtfs.json", {})
    healthcare = cache_read_json("healthcare_points.json", {})
    placemaking = cache_read_json("placemaking_points.json", {})
    traffic = cache_read_json("traffic_volumes.json", {})
    housing = cache_read_json("housing_listings.json", {"type": "FeatureCollection", "features": []})
    if layer == "transportation":
        return {
            "transport_lines": gtfs.get("routes", {"type": "FeatureCollection", "features": []}),
            "transport_stops": gtfs.get("stops", {"type": "FeatureCollection", "features": []}),
            "traffic_roads": traffic.get("roads", {"type": "FeatureCollection", "features": []}),
        }
    if layer == "healthcare":
        return {
            "hospitals": {"type": "FeatureCollection", "features": [
                {"type": "Feature", "properties": {"name": fac["name"], "address": fac["address"], "wait_hours": fac.get("wait_hours")}, "geometry": {"type": "Point", "coordinates": [fac["lon"], fac["lat"]]}}
                for fac in healthcare.get("facilities", [])
            ]},
            "healthcare_points": healthcare.get("poi", {"type": "FeatureCollection", "features": []}),
        }
    if layer == "housing":
        return {"housing_offers": housing}
    if layer == "placemaking":
        return {
            "amenities": placemaking.get("amenities", {"type": "FeatureCollection", "features": []}),
            "trails": placemaking.get("trails", {"type": "FeatureCollection", "features": []}),
        }
    return {}


def layer_options_for(layer: str) -> list[str]:
    return {
        "transportation": ["transit", "traffic"],
        "healthcare": ["access", "wait_time"],
        "housing": ["offers", "rent", "vacancy"],
        "employment": ["unemployment_rate", "employment_rate", "participation"],
        "placemaking": ["amenities", "diversity", "trails"],
    }.get(layer, ["transit"])


def normalize_map_sublayer(layer: str, sublayer: str | None) -> str:
    opts = layer_options_for(layer)
    s = (sublayer or "").strip().lower()
    if s in ("overall", "") or s not in opts:
        return opts[0]
    return s


def score_for_cell_layer(cell: dict[str, Any], layer: str, sublayer: str) -> float:
    if layer == "transportation":
        if sublayer == "transit":
            return float(cell["transportation"]["transit_score"])
        if sublayer == "traffic":
            return normalize_metric(float(cell["transportation"]["avg_aadt"] or 0), 2000.0, 50000.0, "lower")
        return float(cell["transportation"]["transit_score"])
    if layer == "healthcare":
        if sublayer == "access":
            return normalize_metric(float(cell["healthcare"]["avg_hospital_distance_km"]), 0.5, 12.0, "lower")
        if sublayer == "wait_time":
            return normalize_metric(float(cell["healthcare"]["avg_wait_hours"]), 1.0, 12.0, "lower")
        return normalize_metric(float(cell["healthcare"]["avg_hospital_distance_km"]), 0.5, 12.0, "lower")
    if layer == "housing":
        if sublayer == "offers":
            # treat offer density as positive activity
            return clamp(float(cell["housing"]["offer_count"]) * 12.0, 0.0, 100.0)
        if sublayer == "rent":
            return normalize_metric(float(cell["housing"]["local_rent_cad"]), 900.0, 3200.0, "lower")
        if sublayer == "vacancy":
            return normalize_metric(float(cell["housing"]["vacancy_rate_pct"]), 1.0, 6.0, "higher")
        return clamp(float(cell["housing"]["offer_count"]) * 12.0, 0.0, 100.0)
    if layer == "employment":
        if sublayer == "employment_rate":
            return normalize_metric(float(cell["employment"]["employment_rate_pct"]), 50.0, 80.0, "higher")
        if sublayer == "unemployment_rate":
            return normalize_metric(float(cell["employment"]["unemployment_rate_pct"]), 2.0, 12.0, "lower")
        if sublayer == "participation":
            return normalize_metric(float(cell["employment"]["participation_rate_pct"]), 55.0, 80.0, "higher")
        return normalize_metric(float(cell["employment"]["unemployment_rate_pct"]), 2.0, 12.0, "lower")
    if layer == "placemaking":
        if sublayer == "amenities":
            return float(cell["placemaking"].get("amenity_mix_score") or 0)
        if sublayer == "diversity":
            return clamp(float(cell["placemaking"]["amenity_diversity"]) * 12.5, 0.0, 100.0)
        if sublayer == "trails":
            return normalize_metric(float(cell["placemaking"]["trail_km"]), 0.0, 20.0, "higher")
        return clamp(float(cell["placemaking"]["amenity_count"]) * 5.0, 0.0, 100.0)
    return 0.0


def build_map_payload(conn: sqlite3.Connection, layer: str, sublayer: str | None, bbox: tuple[float, float, float, float] | None) -> dict[str, Any]:
    cell_cache = build_cell_cache(conn)
    sublayer_eff = normalize_map_sublayer(layer, sublayer)
    cells = [c for c in cell_cache.get("cells", []) if cell_matches_selection(c, bbox)]
    if not cells:
        cells = cell_cache.get("cells", [])

    heat_features = []
    for cell in cells:
        score = score_for_cell_layer(cell, layer, sublayer_eff)
        props = {
            "id": cell["id"],
            "city": cell["city"],
            "score": round(score, 1),
            "fillColor": color_for_score(score),
            "transport": cell["transportation"],
            "healthcare": cell["healthcare"],
            "housing": cell["housing"],
            "employment": cell["employment"],
            "placemaking": cell["placemaking"],
        }
        heat_features.append({"type": "Feature", "properties": props, "geometry": cell["geometry"]})

    city_cards = city_score_cards(cell_cache)
    city_fc = city_boundary_collection()
    city_features = []
    card_by_city = {c["city"]: c for c in city_cards}
    for feat in city_fc.get("features", []):
        city = feat.get("properties", {}).get("city")
        card = card_by_city.get(city, {})
        city_features.append({"type": "Feature", "properties": {"city": city, "scores": card}, "geometry": feat["geometry"]})

    sel_feature = None
    if bbox:
        sel_poly = selection_polygon(bbox)
        sel_cells = [c for c in cell_cache.get("cells", []) if cell_matches_selection(c, bbox)]
        score = sum(score_for_cell_layer(c, layer, sublayer_eff) for c in sel_cells) / len(sel_cells) if sel_cells else 0.0
        sel_feature = {"type": "FeatureCollection", "features": [{"type": "Feature", "properties": {"label": bbox_label(bbox), "score": round(score, 1), "fillColor": color_for_score(score)}, "geometry": mapping(sel_poly)}]}

    traffic_aadt_legend = [
        {"label": "AADT 0–5k", "color": "#22c55e"},
        {"label": "5–15k", "color": "#eab308"},
        {"label": "15–30k", "color": "#f97316"},
        {"label": "30k+", "color": "#dc2626"},
    ]

    return {
        "layer_options": layer_options_for(layer),
        "active_sublayer": sublayer_eff,
        "legend": [
            {"label": "Lower score", "color": "rgb(220,38,38)"},
            {"label": "Medium score", "color": "rgb(245,158,11)"},
            {"label": "Higher score", "color": "rgb(22,163,74)"},
        ],
        "traffic_aadt_legend": traffic_aadt_legend if layer == "transportation" else None,
        "heat_cells": {"type": "FeatureCollection", "features": heat_features},
        "cities": {"type": "FeatureCollection", "features": city_features},
        "selection": sel_feature,
        "overlays": overlay_payload_for_layer(conn, layer),
    }


# ---------- run pipeline ----------

def run_pipeline(run_id: int) -> None:
    global CURRENT_RUN_ID
    with closing(get_db()) as conn:
        try:
            log_run(conn, run_id, "info", "Starting live refresh.")
            fetch_city_boundaries()
            log_run(conn, run_id, "info", "City boundaries refreshed.")
            fetch_gtfs_payload()
            log_run(conn, run_id, "info", "GRT GTFS refreshed.")
            fetch_traffic_volumes()
            log_run(conn, run_id, "info", "Traffic volumes refreshed.")
            scrape_healthcare_sources()
            log_run(conn, run_id, "info", "Healthcare sources refreshed.")
            scrape_placemaking_sources()
            log_run(conn, run_id, "info", "Placemaking sources refreshed.")
            scrape_city_population_density(conn, run_id)
            scrape_employment_sources(conn, run_id)
            scrape_housing_sources(conn, run_id)
            ensure_nonzero_metrics(conn, run_id)
            refresh_source_previews(conn, run_id)
            build_cell_cache(conn)
            export_region_metrics_snapshot(conn)
            export_sources_registry(conn)
            add_alert(conn, "info", "pipeline", "Data refresh completed", "The scorecard data refresh completed successfully.")
            conn.execute("UPDATE runs SET completed_at = ?, status = ?, summary_json = ? WHERE id = ?", (now_iso(), "completed", json.dumps({"status": "ok"}), run_id))
            conn.commit()
        except Exception as exc:
            log_run(conn, run_id, "error", f"Pipeline failed: {exc}")
            add_alert(conn, "error", "pipeline", "Data refresh failed", str(exc))
            conn.execute("UPDATE runs SET completed_at = ?, status = ?, summary_json = ? WHERE id = ?", (now_iso(), "failed", json.dumps({"error": str(exc)}), run_id))
            conn.commit()
        finally:
            CURRENT_RUN_ID = run_id


def start_pipeline_async() -> int:
    global CURRENT_THREAD, CURRENT_RUN_ID
    with RUN_LOCK:
        with closing(get_db()) as conn:
            row = conn.execute("INSERT INTO runs (started_at, status, summary_json) VALUES (?, ?, ?)", (now_iso(), "running", json.dumps({"status": "running"}))).lastrowid
            conn.commit()
            run_id = int(row)
        CURRENT_RUN_ID = run_id
        t = threading.Thread(target=run_pipeline, args=(run_id,), daemon=True)
        CURRENT_THREAD = t
        t.start()
        return run_id

def seed_minimum_metric_history(conn: sqlite3.Connection) -> None:
    """Offline-safe nonzero defaults so the UI never comes up blank.
    Live scraping overwrites these with source-backed values."""
    has_any = conn.execute("SELECT COUNT(*) FROM metric_snapshots").fetchone()[0]
    if has_any:
        return
    # city-level fallback densities
    for city, density in {"Kitchener": 2100.0, "Waterloo": 1900.0, "Cambridge": 1800.0}.items():
        add_metric_snapshot(conn, "population_density_city", density, "people/km²", ["seed"], {"seed": True, "city": city}, "city", city, observed_at="2021-01-01")
    # housing
    housing = {
        "Kitchener": (1825.0, 4.0, 705000.0),
        "Waterloo": (1945.0, 4.2, 735000.0),
        "Cambridge": (1760.0, 4.1, 685000.0),
    }
    for city, (rent, vac, price) in housing.items():
        add_metric_snapshot(conn, "housing_avg_rent_cad", rent, "CAD", ["seed"], {"seed": True, "city": city}, "city", city, observed_at="2025-10-01")
        add_metric_snapshot(conn, "housing_vacancy_rate_pct", vac, "%", ["seed"], {"seed": True, "city": city}, "city", city, observed_at="2025-10-01")
        add_metric_snapshot(conn, "housing_median_price_cad", price, "CAD", ["seed"], {"seed": True, "city": city}, "city", city, observed_at="2025-10-01")
    add_metric_snapshot(conn, "housing_avg_rent_cad", 1832.0, "CAD", ["seed"], {"seed": True}, "whole", None, observed_at="2025-10-01")
    add_metric_snapshot(conn, "housing_vacancy_rate_pct", 4.1, "%", ["seed"], {"seed": True}, "whole", None, observed_at="2025-10-01")
    add_metric_snapshot(conn, "housing_median_price_cad", 708000.0, "CAD", ["seed"], {"seed": True}, "whole", None, observed_at="2025-10-01")
    # employment history and city metrics
    for i in range(18):
        y = 2024 + ((9 + i) // 12)
        m = ((9 + i) % 12) + 1
        obs = f"{y:04d}-{m:02d}-01T00:00:00+00:00"
        unemp = 6.0 + 0.2 * math.sin(i / 2.0)
        part = 67.0 + 0.3 * math.cos(i / 3.0)
        emp = (100.0 - unemp) * part / 100.0
        add_metric_snapshot(conn, "unemployment_rate_pct", round(unemp, 2), "%", ["seed"], {"seed": True}, "whole", None, observed_at=obs)
        add_metric_snapshot(conn, "labour_participation_pct", round(part, 2), "%", ["seed"], {"seed": True}, "whole", None, observed_at=obs)
        add_metric_snapshot(conn, "employment_rate_pct", round(emp, 2), "%", ["seed"], {"seed": True}, "whole", None, observed_at=obs)
        for city in CITY_NAMES:
            add_metric_snapshot(conn, "unemployment_rate_pct", round(unemp + {"Kitchener": 0.1, "Waterloo": -0.1, "Cambridge": 0.05}[city], 2), "%", ["seed"], {"seed": True, "city": city}, "city", city, observed_at=obs)
            add_metric_snapshot(conn, "labour_participation_pct", round(part, 2), "%", ["seed"], {"seed": True, "city": city}, "city", city, observed_at=obs)
            add_metric_snapshot(conn, "employment_rate_pct", round(emp, 2), "%", ["seed"], {"seed": True, "city": city}, "city", city, observed_at=obs)


# ---------- routes ----------

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/dashboard")
def api_dashboard():
    layer = request.args.get("layer", "transportation")
    bbox = parse_bbox_arg(request.args.get("bbox"))
    with closing(get_db()) as conn:
        summary, metrics = build_summary(conn, bbox)
        return jsonify({
            "summary": summary,
            "metrics": metrics,
            "city_cards": city_score_cards(build_cell_cache(conn)),
            "alerts": rows_to_dicts(conn.execute("SELECT * FROM alerts ORDER BY id DESC LIMIT 25").fetchall()),
        })


@app.route("/api/map")
def api_map():
    layer = request.args.get("layer", "transportation")
    sublayer = request.args.get("sublayer")
    bbox = parse_bbox_arg(request.args.get("bbox"))
    with closing(get_db()) as conn:
        return jsonify(build_map_payload(conn, layer, sublayer, bbox))


@app.route("/api/formulas")
def api_formulas():
    with closing(get_db()) as conn:
        rows = rows_to_dicts(conn.execute("SELECT * FROM formulas ORDER BY title").fetchall())
        for row in rows:
            row["variables"] = json.loads(row.get("variables_json") or "[]")
        return jsonify(rows)


@app.route("/api/formulas/<formula_key>", methods=["PATCH"])
def api_update_formula(formula_key: str):
    data = request.get_json(force=True)
    expr = str(data.get("expression", "")).strip()
    with closing(get_db()) as conn:
        conn.execute("UPDATE formulas SET expression = ? WHERE formula_key = ?", (expr, formula_key))
        conn.commit()
    if (CACHE_DIR / "grid_cells_v10.json").exists():
        (CACHE_DIR / "grid_cells_v10.json").unlink()
    return jsonify({"ok": True})


@app.route("/api/weights/metric/<metric_key>", methods=["PATCH"])
def api_update_metric_weight(metric_key: str):
    data = request.get_json(force=True)
    with closing(get_db()) as conn:
        conn.execute("UPDATE metric_defs SET weight = ? WHERE metric_key = ?", (float(data.get("weight", 1.0)), metric_key))
        conn.commit()
    return jsonify({"ok": True})


@app.route("/api/weights/sector/<sector>", methods=["PATCH"])
def api_update_sector_weight(sector: str):
    data = request.get_json(force=True)
    with closing(get_db()) as conn:
        conn.execute("UPDATE sector_weights SET weight = ? WHERE sector = ?", (float(data.get("weight", 1.0)), sector))
        conn.commit()
    return jsonify({"ok": True})


@app.route("/api/sources")
def api_sources():
    with closing(get_db()) as conn:
        return jsonify(rows_to_dicts(conn.execute("SELECT * FROM sources ORDER BY sector, name").fetchall()))


@app.route("/api/sources", methods=["POST"])
def api_add_source():
    data = request.get_json(force=True)
    with closing(get_db()) as conn:
        conn.execute(
            "INSERT INTO sources (name, sector, source_url, access_method, parser_type, update_frequency, use_llm, active, notes, status) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, 'manual')",
            (
                data.get("name", "New source"),
                data.get("sector", "Placemaking"),
                data.get("source_url", ""),
                data.get("access_method", "html"),
                data.get("parser_type", "html"),
                data.get("update_frequency", "weekly"),
                int(data.get("use_llm", 0)),
                data.get("notes", ""),
            ),
        )
        conn.commit()
    return jsonify({"ok": True})


@app.route("/api/sources/<int:source_id>", methods=["PATCH"])
def api_patch_source(source_id: int):
    data = request.get_json(force=True)
    fields = []
    values = []
    for key in ["update_frequency", "use_llm", "active", "notes", "status"]:
        if key in data:
            fields.append(f"{key} = ?")
            values.append(data[key])
    if fields:
        with closing(get_db()) as conn:
            conn.execute(f"UPDATE sources SET {', '.join(fields)} WHERE id = ?", (*values, source_id))
            conn.commit()
    return jsonify({"ok": True})


@app.route("/api/sources/<int:source_id>", methods=["DELETE"])
def api_delete_source(source_id: int):
    with closing(get_db()) as conn:
        conn.execute("DELETE FROM sources WHERE id = ?", (source_id,))
        conn.commit()
    return jsonify({"ok": True})


@app.route("/api/weight_meta")
def api_weight_meta():
    with closing(get_db()) as conn:
        return jsonify(weight_meta(conn))


@app.route("/api/plots")
def api_plots():
    metric_key = request.args.get("metric_key", "housing_median_price_cad")
    region_scope = request.args.get("region_scope", "whole")
    region_id = request.args.get("region_id")
    start = request.args.get("start")
    end = request.args.get("end")
    with closing(get_db()) as conn:
        params = [metric_key, region_scope]
        sql = "SELECT * FROM metric_snapshots WHERE metric_key = ? AND region_scope = ?"
        if region_scope == "city":
            sql += " AND region_id = ?"
            params.append(region_id)
        else:
            sql += " AND region_id IS NULL"
        if start:
            sql += " AND observed_at >= ?"
            params.append(normalize_observed_at(start))
        if end:
            sql += " AND observed_at <= ?"
            params.append(normalize_observed_at(end))
        sql += " ORDER BY observed_at"
        rows = rows_to_dicts(conn.execute(sql, tuple(params)).fetchall())
        fallback_whole = False
        if region_scope == "city" and not rows:
            params = [metric_key]
            sql = "SELECT * FROM metric_snapshots WHERE metric_key = ? AND region_scope = 'whole' AND region_id IS NULL"
            if start:
                sql += " AND observed_at >= ?"
                params.append(normalize_observed_at(start))
            if end:
                sql += " AND observed_at <= ?"
                params.append(normalize_observed_at(end))
            sql += " ORDER BY observed_at"
            rows = rows_to_dicts(conn.execute(sql, tuple(params)).fetchall())
            fallback_whole = True
        return jsonify({"points": rows, "fallback_whole": fallback_whole})


@app.route("/api/logs")
def api_logs():
    with closing(get_db()) as conn:
        run_id = CURRENT_RUN_ID
        if run_id is None:
            rows = rows_to_dicts(conn.execute("SELECT * FROM logs ORDER BY id DESC LIMIT 50").fetchall())
        else:
            rows = rows_to_dicts(conn.execute("SELECT * FROM logs WHERE run_id = ? ORDER BY id DESC LIMIT 200", (run_id,)).fetchall())
        return jsonify(rows)


@app.route("/api/llm_extractions")
def api_llm_extractions():
    with closing(get_db()) as conn:
        rows = rows_to_dicts(conn.execute("SELECT * FROM llm_extractions ORDER BY id DESC LIMIT 50").fetchall())
        return jsonify(rows)


@app.route("/api/alerts")
def api_alerts():
    with closing(get_db()) as conn:
        return jsonify(rows_to_dicts(conn.execute("SELECT * FROM alerts ORDER BY id DESC LIMIT 50").fetchall()))


def _catalog_float_from_export(whole: dict[str, Any], key: str) -> float | None:
    w = whole.get(key)
    if w and w.get("value") is not None:
        try:
            return float(w["value"])
        except Exception:
            return None
    return None


def _avg_aadt_traffic_segments() -> float | None:
    t = cache_read_json("traffic_volumes.json", {})
    vals: list[float] = []
    for f in t.get("roads", {}).get("features", []):
        a = f.get("properties", {}).get("AADT")
        if a in (None, ""):
            continue
        try:
            vals.append(float(a))
        except Exception:
            continue
    if not vals:
        return None
    return round(sum(vals) / len(vals), 0)


def build_data_catalog_payload() -> dict[str, Any]:
    rm = cache_read_json(REGION_METRICS_JSON, {})
    whole = rm.get("whole") or {}
    snap_n = rm.get("metric_snapshots_rows")
    metrics_updated = rm.get("updated_at")
    src_blob = cache_read_json(SOURCES_REGISTRY_JSON, {})
    src_rows = src_blob.get("sources") or []

    def _mtime(name: str) -> str | None:
        p = CACHE_DIR / name
        if not p.exists():
            return None
        return datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    freshness = {
        "city_boundaries": _mtime("city_boundaries.json"),
        "grt_gtfs": _mtime("transport_gtfs.json"),
        "traffic_volumes": _mtime("traffic_volumes.json"),
        "healthcare_points": _mtime("healthcare_points.json"),
        "placemaking_points": _mtime("placemaking_points.json"),
        "housing_listings": _mtime("housing_listings.json"),
        "grid_cells": _mtime("grid_cells_v10.json"),
    }

    hc = cache_read_json("healthcare_points.json", {})
    hospital_rows: list[dict[str, Any]] = []
    for fac in hc.get("facilities") or []:
        wh = fac.get("wait_hours")
        href = fac.get("er_watch_hospital_url") or "https://www.er-watch.ca/"
        hospital_rows.append({
            "label": fac.get("name", "Hospital"),
            "value": f"{float(wh):.1f} h ER wait" if wh is not None else "—",
            "href": href,
            "note": fac.get("address", ""),
        })

    vac = _catalog_float_from_export(whole, "housing_vacancy_rate_pct")
    rent2 = _catalog_float_from_export(whole, "housing_avg_rent_cad")
    rent1 = _catalog_float_from_export(whole, "housing_avg_rent_1br_cad")
    rent3 = _catalog_float_from_export(whole, "housing_avg_rent_3br_cad")
    price = _catalog_float_from_export(whole, "housing_median_price_cad")
    listings = _catalog_float_from_export(whole, "housing_active_listings_proxy")

    unemp = _catalog_float_from_export(whole, "unemployment_rate_pct")
    emp = _catalog_float_from_export(whole, "employment_rate_pct")
    part = _catalog_float_from_export(whole, "labour_participation_pct")

    avg_aadt = _avg_aadt_traffic_segments()
    cells = cache_read_json("grid_cells_v10.json", {}).get("cells") or []
    avg_cell_aadt = None
    if cells:
        aadts = [float(c["transportation"]["avg_aadt"]) for c in cells if c.get("transportation", {}).get("avg_aadt") is not None]
        if aadts:
            avg_cell_aadt = round(sum(aadts) / len(aadts), 0)

    sections: list[dict[str, Any]] = [
        {
            "id": "healthcare",
            "title": "Healthcare",
            "intro": "Hospital pins are geocoded from known WRHN / Cambridge addresses. ER waits are scraped from ER Watch (Next.js payload and page text), then assigned by hospital URL slug and name tokens (Grand River ↔ Midtown, St. Mary’s ↔ Queen St, Cambridge Memorial ↔ Cambridge slug). OSM adds doctors, dentists, and clinics for access density.",
            "rows": hospital_rows,
            "bullets": [
                {"label": "ER Watch — live ER waits (Ontario)", "value": "Hospital pages + wait parser", "href": "https://www.er-watch.ca/"},
                {"label": "Healthcare POI overlay", "value": "OpenStreetMap via Overpass API", "href": "https://overpass-api.de/api/interpreter"},
                {"label": "WRHN sites (reference)", "value": "Grand River Hospital", "href": "https://www.grhosp.on.ca/"},
            ],
        },
        {
            "id": "transportation",
            "title": "Transportation",
            "intro": "GRT routes and stops come from the published GTFS static feed. Road volumes use the Region of Waterloo open AADT layer; the app caches GeoJSON and blends segment AADT into grid cells for the score map.",
            "rows": [],
            "bullets": [
                {"label": "GRT GTFS (routes & stops)", "value": "Regional static feed", "href": "https://webapps.regionofwaterloo.ca/api/grt-routes/api/staticfeeds/1"},
                {"label": "Traffic volumes / AADT (GeoJSON)", "value": "Region of Waterloo open data", "href": "https://rowopendata-rmw.opendata.arcgis.com/items/426089c5166c4f8f8f4000acb2fef840"},
                {"label": "Mean AADT (cached road segments)", "value": f"{avg_aadt:,.0f} vehicles/day" if avg_aadt is not None else "—", "href": ""},
                {"label": "Mean AADT (grid cells, regional blend)", "value": f"{avg_cell_aadt:,.0f} vehicles/day" if avg_cell_aadt is not None else "—", "href": ""},
            ],
        },
        {
            "id": "housing",
            "title": "Housing",
            "intro": "Rental and vacancy figures follow CMHC’s Kitchener–Cambridge–Waterloo zone; sale metrics align with WRAR-style market stats where scraped. Numbers below are the latest values exported to cache (same run as live refresh).",
            "rows": [],
            "bullets": [
                {"label": "Vacancy rate (KCW, CMHC)", "value": f"{vac:.2f} %" if vac is not None else "—", "href": "https://www03.cmhc-schl.gc.ca/hmip-pimh/en/Profile?a=20&geoId=0850&t=3"},
                {"label": "Average rent (headline / 2BR proxy)", "value": f"${rent2:,.0f} / mo" if rent2 is not None else "—", "href": "https://www03.cmhc-schl.gc.ca/hmip-pimh/en/Profile?a=20&geoId=0850&t=3"},
                {"label": "Average rent (1 bedroom)", "value": f"${rent1:,.0f} / mo" if rent1 is not None else "—", "href": "https://www03.cmhc-schl.gc.ca/hmip-pimh/en/TableMapChart?id=0850&t=3"},
                {"label": "Average rent (3 bedroom)", "value": f"${rent3:,.0f} / mo" if rent3 is not None else "—", "href": "https://www03.cmhc-schl.gc.ca/hmip-pimh/en/TableMapChart?id=0850&t=3"},
                {"label": "Median / proxy home price", "value": f"${price:,.0f}" if price is not None else "—", "href": "https://wrar.ca/category/market-stats/"},
                {"label": "Active listing points (proxy count)", "value": f"{listings:,.0f}" if listings is not None else "—", "href": "https://wrar.ca/category/market-stats/"},
            ],
        },
        {
            "id": "employment",
            "title": "Employment",
            "intro": "Labour market rates are taken from Statistics Canada CMA-level series where available, with seeded history so charts never start empty before a live scrape.",
            "rows": [],
            "bullets": [
                {"label": "Unemployment rate", "value": f"{unemp:.2f} %" if unemp is not None else "—", "href": "https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1410045901"},
                {"label": "Employment rate", "value": f"{emp:.2f} %" if emp is not None else "—", "href": "https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1410045901"},
                {"label": "Labour force participation", "value": f"{part:.2f} %" if part is not None else "—", "href": "https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1410045901"},
            ],
        },
        {
            "id": "placemaking",
            "title": "Placemaking",
            "intro": "Amenity mix uses distinct OSM amenity/shop/leisure tags in each buffer: fewer than three types reads as red, four to five as moderate green, and about ten distinct types as strong green (raw counts nudge the score slightly). Trails and parks add livability context.",
            "rows": [],
            "bullets": [
                {"label": "Amenities & trails (Overpass)", "value": "OSM query in region bbox", "href": "https://overpass-api.de/api/interpreter"},
                {"label": "City boundaries", "value": "Nominatim polygons", "href": "https://nominatim.openstreetmap.org/search"},
            ],
        },
    ]

    return {
        "title": "Waterloo Region scorecard — data catalog",
        "region": "Kitchener–Cambridge–Waterloo (Regional Municipality of Waterloo)",
        "summary": (
            "Figures below are read from JSON exports in cache/ (region_metrics.json and sources_registry.json). "
            "They are refreshed when you run live scraping or on first app start — no live SQL reads in this view."
        ),
        "freshness": freshness,
        "metric_snapshots_rows": snap_n,
        "metrics_export_updated_at": metrics_updated,
        "sources_export_updated_at": src_blob.get("updated_at"),
        "sections": sections,
        "sources": src_rows,
    }


@app.route("/api/data-catalog")
def api_data_catalog():
    return jsonify(build_data_catalog_payload())


@app.route("/api/docs")
def api_docs():
    with closing(get_db()) as conn:
        rows = rows_to_dicts(conn.execute("SELECT * FROM sources ORDER BY sector, name").fetchall())
        docs = [{
            "name": r["name"],
            "sector": r["sector"],
            "url": r["source_url"],
            "access_method": r["access_method"],
            "frequency": r["update_frequency"],
            "notes": r["notes"],
            "preview_text": r["preview_text"],
            "active": bool(r["active"]),
        } for r in rows]
        return jsonify(docs)


@app.route("/api/run", methods=["POST"])
def api_run():
    run_id = start_pipeline_async()
    return jsonify({"ok": True, "run_id": run_id})


@app.route("/api/run_status")
def api_run_status():
    with closing(get_db()) as conn:
        if CURRENT_RUN_ID is None:
            return jsonify({"status": "idle"})
        row = conn.execute("SELECT * FROM runs WHERE id = ?", (CURRENT_RUN_ID,)).fetchone()
        if not row:
            return jsonify({"status": "idle"})
        return jsonify({"status": row["status"], "run_id": row["id"], "started_at": row["started_at"], "completed_at": row["completed_at"]})


@app.route("/api/reset", methods=["POST"])
def api_reset():
    # clear caches only
    for path in CACHE_DIR.glob("*.json"):
        try:
            path.unlink()
        except Exception:
            pass
    return jsonify({"ok": True})


@app.route("/api/clear_db", methods=["POST"])
def api_clear_db():
    if DB_PATH.exists():
        DB_PATH.unlink()
    init_db()
    return jsonify({"ok": True})


def bootstrap() -> None:
    init_db()
    with closing(get_db()) as conn:
        fetch_city_boundaries()
        seed_minimum_metric_history(conn)
        create_mock_housing_offers(conn)
        export_region_metrics_snapshot(conn)
        export_sources_registry(conn)


bootstrap()

if __name__ == "__main__":
    app.run(debug=True)
