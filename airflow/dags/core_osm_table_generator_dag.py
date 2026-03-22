import json
import os
import re
import logging
import time
from datetime import datetime, timezone, timedelta

import pandas as pd
import geopandas as gpd
import osmnx as ox
from psycopg2.extras import execute_values

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.operators.python import get_current_context
from airflow.operators.empty import EmptyOperator

logger = logging.getLogger("airflow.task")


# ======================
# CONFIG
# ======================
TARGET_SCHEMA = "berlin_poi"
BERLIN_PLACE_QUERY = "Berlin, Germany"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_DIR = os.path.join(BASE_DIR, "config")

CORE_COLUMNS_PATH = os.path.join(CONFIG_DIR, "core_columns.json")
OSM_TABLES_PATH = os.path.join(CONFIG_DIR, "osm_tables.json")
LOR_GEOJSON_PATH = os.path.join(CONFIG_DIR, "lor_ortsteile.geojson")


# ======================
# LOAD CONFIGS
# ======================
with open(CORE_COLUMNS_PATH, "r", encoding="utf-8") as f:
    core_config = json.load(f)

with open(OSM_TABLES_PATH, "r", encoding="utf-8") as f:
    osm_config = json.load(f)

must_columns = core_config["must_columns"]
common_columns = core_config["common_columns"]

MUST_COL_NAMES = [c["name"] for c in must_columns]
COMMON_COL_NAMES = [c["name"] for c in common_columns]
# Keep order, remove duplicates (important for stable final column order)
CORE_AND_COMMON = list(dict.fromkeys(MUST_COL_NAMES + COMMON_COL_NAMES))

# ========================================
# HELPERS: NORMALIZER + COLLISION CHECKER
# ========================================
_norm_re = re.compile(r"[^a-z0-9_]+")

def normalize_col(name: str) -> str:
    s = name.strip().lower()
    s = _norm_re.sub("_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s

def ensure_no_collisions(raw_names: list[str], context: str) -> dict[str, str]:
    """
    Returns mapping raw->normalized, and raises if two raw names normalize to same result.
    """
    mapping = {}
    seen = {}
    for raw in raw_names:
        norm = normalize_col(raw)
        if norm in seen and seen[norm] != raw:
            raise ValueError(
                f"Normalization collision in {context}: "
                f"'{seen[norm]}' and '{raw}' -> '{norm}'"
            )
        seen[norm] = raw
        mapping[raw] = norm
    return mapping

# ======================
# HELPERS: DDL + METADATA
# ======================
def build_columns_sql(unique_columns):
    """
    Build CREATE TABLE column definitions:
    - core+common from core_columns.json
    - plus table-specific unique columns from osm_tables.json
    """
    columns = []
    core_names = set(CORE_AND_COMMON)

    for col in must_columns + common_columns:
        if col["name"] == "id":
            columns.append(f'{col["name"]} {col["type"]} PRIMARY KEY')
        else:
            columns.append(f'{col["name"]} {col["type"]}')

    for col in unique_columns:
        if col["name"] not in core_names:
            columns.append(f'{normalize_col(col["name"])} {col["type"]}')

    return ",\n    ".join(columns)


def ensure_postgis(cur):
    """Safe even if already installed."""
    cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")


def ensure_metadata_table(cur):
    """
    Metadata table:
    one row per table per run_timestamp
    """
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.osm_ingestion_log (
            run_timestamp TIMESTAMPTZ NOT NULL,
            table_name TEXT NOT NULL,
            tag_key TEXT NOT NULL,
            tag_value TEXT NOT NULL,
            records_fetched INTEGER NOT NULL DEFAULT 0,
            records_inserted INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (run_timestamp, table_name)
        );
        """
    )


def insert_metadata_row(cur, run_ts, table_name, tag_key, tag_value, records_fetched, records_inserted):
    """Upsert metadata row for (run_timestamp, table_name)."""
    cur.execute(
        f"""
        INSERT INTO {TARGET_SCHEMA}.osm_ingestion_log
            (run_timestamp, table_name, tag_key, tag_value, records_fetched, records_inserted)
        VALUES
            (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (run_timestamp, table_name) DO UPDATE SET
            tag_key = EXCLUDED.tag_key,
            tag_value = EXCLUDED.tag_value,
            records_fetched = EXCLUDED.records_fetched,
            records_inserted = EXCLUDED.records_inserted;
        """,
        (run_ts, table_name, tag_key, tag_value, records_fetched, records_inserted),
    )


# ======================
# HELPERS: OSM FETCH
# ======================
def normalize_tags(table_cfg):
    """
    Convert the tags list from osm_tables.json into the dict format expected by OSMnx.
    Supports:
    - multiple entries for same key (dedup list)
    - values as scalar or list
    """
    tags = {}
    for t in table_cfg.get("tags", []) or []:
        k = t["key"]
        v = t["value"]
        if k not in tags:
            tags[k] = v
        else:
            existing = tags[k]
            if not isinstance(existing, list):
                existing = [existing]
            if isinstance(v, list):
                existing.extend(v)
            else:
                existing.append(v)

            # de-dup preserve order
            seen = set()
            dedup = []
            for x in existing:
                if x not in seen:
                    seen.add(x)
                    dedup.append(x)
            tags[k] = dedup

    return tags


def fetch_osm_geometries(table_cfg):
    """Fetch OSM features for Berlin using normalized tags."""
    tags = normalize_tags(table_cfg)
    return ox.features_from_place(BERLIN_PLACE_QUERY, tags=tags)


# ======================
# STAGE 0 — ID
# ======================
def ensure_id(gdf):
    """
    Decision: PK is 'element_id' + '_' + 'osm_id' from OSMnx MultiIndex.
    Drop the index to avoid downstream index-related issues.
    """
    if gdf is None or len(gdf) == 0:
        return gdf

    gdf = gdf.copy()

    # Remove any pre-existing id column to avoid ambiguity
    if "id" in gdf.columns:
        gdf = gdf.drop(columns=["id"])

    # OSMnx output: MultiIndex (element, id)
    element = gdf.index.get_level_values(0).astype(str)
    osm_id = gdf.index.get_level_values(1).astype(str)
    gdf["id"] = element + "_" + osm_id

    return gdf.reset_index(drop=True)


# ======================
# STAGE 1 — GEOMETRY
# ======================
def prepare_geometry(gdf):
    """
    - Ensure CRS is EPSG:4326
    - Convert geometry to Point:
        - keep Points
        - for non-Points use representative_point()
    - Derive latitude/longitude from Point geometry
    """
    if gdf is None or len(gdf) == 0:
        return gdf

    if getattr(gdf, "crs", None) is None:
        gdf = gdf.set_crs(epsg=4326)
    else:
        gdf = gdf.to_crs(epsg=4326)

    def to_point(geom):
        if geom is None:
            return None
        try:
            if geom.geom_type == "Point":
                return geom
            return geom.representative_point()
        except Exception:
            return None

    gdf = gdf.copy()
    gdf["geometry"] = gdf["geometry"].apply(to_point)
    gdf = gdf[~gdf["geometry"].isna()].copy()

    gdf["latitude"] = gdf.geometry.y
    gdf["longitude"] = gdf.geometry.x

    return gdf


# ======================
# STAGE 2 — CORE TRANSFORMS
# ======================
def _pick_first_non_null(gdf, candidates):
    """Return the first non-null value among candidate columns (left-to-right)."""
    existing = [c for c in candidates if c in gdf.columns]
    if not existing:
        return None
    s = gdf[existing[0]].copy()
    for c in existing[1:]:
        s = s.fillna(gdf[c])
    return s


def transform_core_columns(gdf, table_name):
    """
    Core transforms :
    - id, name
    - addr_* (street, housenumber, postcode)
    - website / phone / email fallbacks
    - wheelchair boolean
    - last_updated timestamp
    Spatial fields are placeholders here; Stage 3 fills them.
    """
    if gdf is None or len(gdf) == 0:
        return None

    gdf = gdf.copy()

    # name
    if "name" not in gdf.columns:
        gdf["name"] = None
    gdf["name"] = gdf["name"].fillna(f"unknown_{table_name}")

    # address
    gdf["addr_housenumber"] = gdf["addr:housenumber"] if "addr:housenumber" in gdf.columns else None
    gdf["addr_street"] = gdf["addr:street"] if "addr:street" in gdf.columns else None
    gdf["addr_postcode"] = gdf["addr:postcode"] if "addr:postcode" in gdf.columns else None

    # website/phone/email fallbacks
    website = _pick_first_non_null(gdf, ["contact:website", "website"])
    gdf["website"] = website if website is not None else None

    phone = _pick_first_non_null(gdf, ["contact:phone", "phone"])
    gdf["phone_number"] = phone if phone is not None else None

    email = _pick_first_non_null(gdf, ["contact:email", "email"])
    gdf["email"] = email if email is not None else None

    # wheelchair boolean
    if "wheelchair" in gdf.columns:
        gdf["wheelchair"] = gdf["wheelchair"].astype(str).str.lower().isin(["yes", "limited"])
    else:
        gdf["wheelchair"] = False

    # last_updated
    gdf["last_updated"] = datetime.now(timezone.utc)

    # spatial placeholders (Stage 3 will fill)
    gdf["district"] = None
    gdf["district_id"] = None
    gdf["neighborhood"] = None
    gdf["neighborhood_id"] = None

    # Ensure all core/common exist
    for col in CORE_AND_COMMON:
        if col not in gdf.columns:
            gdf[col] = None

    return gdf[CORE_AND_COMMON].copy()


# ======================
# STAGE 3 — LOR SPATIAL JOIN
# ======================
DISTRICT_MAPPING = {
    "Mitte": "11001001",
    "Friedrichshain-Kreuzberg": "11002002",
    "Pankow": "11003003",
    "Charlottenburg-Wilmersdorf": "11004004",
    "Spandau": "11005005",
    "Steglitz-Zehlendorf": "11006006",
    "Tempelhof-Schöneberg": "11007007",
    "Neukölln": "11008008",
    "Treptow-Köpenick": "11009009",
    "Marzahn-Hellersdorf": "11010010",
    "Lichtenberg": "11011011",
    "Reinickendorf": "11012012",
}


def _load_lor_gdf():
    """Load LOR polygons and keep only needed fields."""
    lor = gpd.read_file(LOR_GEOJSON_PATH).to_crs(epsg=4326)
    return lor[["BEZIRK", "OTEIL", "spatial_name", "geometry"]].copy()


def spatial_join_lor(gdf_points, table_name: str | None = None):
    """
    Spatial join points with LOR polygons.

    Contract (drop unmatched):
    Returns a pandas DataFrame with exactly:
        ['id', 'district', 'district_id', 'neighborhood', 'neighborhood_id']

    Rules:
    - Only matched rows are returned (unmatched are dropped)
    - One row per id

    Autonomous version:
    - district from BEZIRK
    - neighborhood_id from spatial_name
    - neighborhood name from OTEIL
    """
    prefix = f"{table_name} " if table_name else ""

    if gdf_points is None or len(gdf_points) == 0:
        logger.info("%sLOR JOIN: empty input -> returning empty mapping", prefix)
        return pd.DataFrame(
            columns=["id", "district", "district_id", "neighborhood", "neighborhood_id"]
        )

    lor = _load_lor_gdf()

    pts = gdf_points[["id", "geometry"]].drop_duplicates(subset=["id"]).copy()
    logger.info(
        "%sLOR JOIN: points_in=%s points_unique_ids=%s",
        prefix,
        len(gdf_points),
        len(pts),
    )

    joined = gpd.sjoin(
        pts,
        lor,
        how="left",
        predicate="within",
    ).drop(columns=["index_right"], errors="ignore")

    # Rename to final contract names
    joined["district"] = joined["BEZIRK"]
    joined["neighborhood"] = joined["OTEIL"]
    joined["neighborhood_id"] = joined["spatial_name"]

    matched_before_drop = joined["district"].notna().sum()
    dropped = len(joined) - matched_before_drop

    # Drop unmatched (your chosen rule)
    joined = joined[joined["district"].notna()].copy()

    logger.info(
        "%sLOR JOIN: matched=%s dropped_unmatched=%s",
        prefix,
        matched_before_drop,
        dropped,
    )

    # district_id mapping (keep NULL if mapping fails)
    joined["district_id"] = joined["district"].map(DISTRICT_MAPPING)
    null_district_ids = joined["district_id"].isna().sum()
    joined.loc[joined["district_id"].isna(), "district_id"] = None

    if null_district_ids:
        logger.warning(
            "%sLOR JOIN: district_id mapping missing for %s rows",
            prefix,
            null_district_ids,
        )

    out = joined[["id", "district", "district_id", "neighborhood", "neighborhood_id"]].copy()

    # Enforce one row per id (defensive)
    out = out.drop_duplicates(subset=["id"], keep="first")

    logger.info(
        "%sLOR JOIN: output_rows=%s (one row per id enforced)",
        prefix,
        len(out),
    )

    return out

# =========================
# STAGE 4 — UNIQUE COLUMNS
# =========================
def build_unique_columns_df(gdf_raw, unique_columns):
    """
    Return df with columns: ['id'] + normalized unique column names.
    Reads from raw OSM columns, then renames to normalized.
    """
    raw_unique = [c["name"] for c in (unique_columns or [])]
    raw_to_norm = ensure_no_collisions(raw_unique, context="unique_columns")

    norm_unique = [raw_to_norm[r] for r in raw_unique]

    if gdf_raw is None or len(gdf_raw) == 0:
        df_uniques = pd.DataFrame({"id": []})
        for c in norm_unique:
            df_uniques[c] = None
        return df_uniques[["id"] + norm_unique]

    # Build a rename map only for columns we care about that exist in raw
    rename_map = {raw: raw_to_norm[raw] for raw in raw_unique if raw in gdf_raw.columns}

    # Select existing raw cols, then rename to normalized
    existing_raw = ["id"] + list(rename_map.keys())
    df_uniques = gdf_raw[existing_raw].copy().rename(columns=rename_map)

    # Ensure all normalized unique cols exist
    for c in norm_unique:
        if c not in df_uniques.columns:
            df_uniques[c] = None

    return df_uniques[["id"] + norm_unique]

# ======================
# STAGE 5 — REPLACE LOAD
# ======================
def replace_load(conn, table_name, df, all_columns):
    """
    Replace strategy:
      - TRUNCATE target
      - INSERT all rows
    Geometry insert via ST_SetSRID(ST_GeomFromText(wkt),4326).
    """
    if df is None or len(df) == 0:
        return 0

    df = df.copy()

    # Keep geometry column as shapely; create temp WKT only for insert payload.
    df["_geometry_wkt"] = df["geometry"].apply(lambda g: g.wkt if hasattr(g, "wkt") else g)

    cols = all_columns[:]  # ordered list
    col_list_sql = ", ".join(cols)

    values_template_parts = []
    for c in cols:
        if c == "geometry":
            values_template_parts.append("ST_SetSRID(ST_GeomFromText(%s), 4326)")
        else:
            values_template_parts.append("%s")
    values_template = "(" + ", ".join(values_template_parts) + ")"

    records = []
    for _, row in df.iterrows():
        row_values = []
        for c in cols:
            if c == "geometry":
                row_values.append(row["_geometry_wkt"])
            else:
                row_values.append(row[c])
        records.append(tuple(row_values))

    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {TARGET_SCHEMA}.{table_name};")
        execute_values(
            cur,
            f"INSERT INTO {TARGET_SCHEMA}.{table_name} ({col_list_sql}) VALUES %s",
            records,
            template=values_template,
            page_size=1000,
        )

    return len(df)

# ======================
# ENGINE — ONE TABLE
# ======================
def process_one_table(conn, cur, run_ts, table_cfg):
    """
    Process exactly one table config (one "table ingestion unit") using the existing
    Stage 0–5 logic and the same metadata logging behavior.

    Returns a small summary dict for logging/observability.
    """
    table_name = table_cfg["table_name"]
    unique_columns = table_cfg.get("unique_columns", [])

    # Metadata strategy (multi-key/multi-value)
    normalized_tags = normalize_tags(table_cfg)
    tag_key = ",".join(sorted(normalized_tags.keys()))
    tag_value = json.dumps(normalized_tags, ensure_ascii=False, sort_keys=True)

    records_fetched = 0
    records_inserted = 0

    t0 = datetime.now(timezone.utc)
    logger.info("START: %s run_ts=%s", table_name, run_ts)

    try:
        # DDL
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.{table_name} (
            {build_columns_sql(unique_columns)}
        );
        """
        cur.execute(create_table_sql)
        conn.commit()
        logger.info("DDL OK: %s", table_name)

        # FETCH
        gdf_raw = fetch_osm_geometries(table_cfg)
        records_fetched = len(gdf_raw)
        logger.info("FETCH OK: %s -> %s raw records", table_name, records_fetched)

        # =========================
        # STAGE 0 — ID
        # =========================
        gdf_raw = ensure_id(gdf_raw)

        # =========================
        # STAGE 1 — GEOMETRY
        # =========================
        gdf_geom = prepare_geometry(gdf_raw)

        # =========================
        # STAGE 2 — CORE TRANSFORMS
        # =========================
        df_core = transform_core_columns(gdf_geom, table_name)
        if df_core is None or len(df_core) == 0:
            # =========================
            # STAGE 6 — METADATA INSERT
            # =========================
            insert_metadata_row(
                cur,
                run_ts,
                table_name,
                tag_key,
                tag_value,
                records_fetched,
                0,
            )
            conn.commit()
            elapsed_s = (datetime.now(timezone.utc) - t0).total_seconds()
            logger.info("END: %s status=%s fetched=%s inserted=%s elapsed_s=%.2f",
                        table_name, "ok_empty_core", records_fetched, 0, elapsed_s)
            return {
                "table_name": table_name,
                "tag_key": tag_key,
                "tag_value": tag_value,
                "records_fetched": records_fetched,
                "records_inserted": 0,
                "status": "ok_empty_core",
            }

        # =========================
        # STAGE 3 — LOR SPATIAL JOIN
        # =========================
        spatial_df = spatial_join_lor(gdf_geom, table_name)
        if spatial_df is None or len(spatial_df) == 0:
            insert_metadata_row(
                cur,
                run_ts,
                table_name,
                tag_key,
                tag_value,
                records_fetched,
                0,
            )
            conn.commit()
            elapsed_s = (datetime.now(timezone.utc) - t0).total_seconds()
            logger.info("END: %s status=%s fetched=%s inserted=%s elapsed_s=%.2f",
                        table_name, "ok_empty_lor", records_fetched, 0, elapsed_s)
            return {
                "table_name": table_name,
                "tag_key": tag_key,
                "tag_value": tag_value,
                "records_fetched": records_fetched,
                "records_inserted": 0,
                "status": "ok_empty_lor",
            }

        # Remove Stage 2 placeholders to avoid collisions/suffixes
        df_core = df_core.drop(
            columns=["district", "district_id", "neighborhood", "neighborhood_id"],
            errors="ignore",
        )

        # Enforce the "drop unmatched" rule via INNER JOIN
        df_core = df_core.merge(spatial_df, on="id", how="inner")

        # =========================
        # STAGE 4 — UNIQUE COLUMNS
        # =========================
        raw_unique_names = [c["name"] for c in unique_columns]
        raw_to_norm = ensure_no_collisions(raw_unique_names, context=f"table={table_name} unique_columns")
        unique_names = [raw_to_norm[r] for r in raw_unique_names]

        # Build unique dataframe (id + all unique columns guaranteed)
        df_uniques = build_unique_columns_df(gdf_raw, unique_columns)

        # Merge by id
        df_final = df_core.merge(df_uniques, on="id", how="left")

        # Final column order = core/common + uniques
        final_columns = CORE_AND_COMMON + [c for c in unique_names if c not in CORE_AND_COMMON]

        # Ensure all final columns exist (safety for schema evolution)
        for c in final_columns:
            if c not in df_final.columns:
                df_final[c] = None

        df_final = df_final[final_columns]

        # =========================
        # STAGE 5 — REPLACE LOAD
        # =========================
        records_inserted = replace_load(conn, table_name, df_final, final_columns)
        conn.commit()

        # =========================
        # STAGE 6 — METADATA INSERT
        # =========================
        insert_metadata_row(
            cur,
            run_ts,
            table_name,
            tag_key,
            tag_value,
            records_fetched,
            records_inserted,
        )
        conn.commit()

        logger.info("LOAD OK: %s -> inserted %s", table_name, records_inserted)
        elapsed_s = (datetime.now(timezone.utc) - t0).total_seconds()
        logger.info("END: %s status=%s fetched=%s inserted=%s elapsed_s=%.2f",
                    table_name, "ok", records_fetched, records_inserted, elapsed_s)
        return {
            "table_name": table_name,
            "tag_key": tag_key,
            "tag_value": tag_value,
            "records_fetched": records_fetched,
            "records_inserted": records_inserted,
            "status": "ok",
        }

    except Exception as e:
        conn.rollback()

        # Still write metadata row (inserted=0) for this table/run if possible
        try:
            insert_metadata_row(
                cur,
                run_ts,
                table_name,
                tag_key,
                tag_value,
                records_fetched,
                0,
            )
            conn.commit()
        except Exception:
            conn.rollback()

        logger.exception("FAILED: %s", table_name)
        elapsed_s = (datetime.now(timezone.utc) - t0).total_seconds()
        logger.info("END: %s status=%s fetched=%s inserted=%s elapsed_s=%.2f",
                    table_name, "failed", records_fetched, 0, elapsed_s)
        return {
            "table_name": table_name,
            "tag_key": tag_key,
            "tag_value": tag_value,
            "records_fetched": records_fetched,
            "records_inserted": 0,
            "status": "failed",
            "error": str(e),
        }

# ======================
# MAIN
# ======================

# ======================
# AIRFLOW DAG
# ======================
# Airflow-only DAG implementation.
# Must be importable by scheduler (no local entrypoint).


# ======================
# RUNTIME STRATEGY CONTROLS (tunable without code edits)
# ======================
# Set these via Docker env (recommended) or default values apply.
#
# Pure-parallel knobs:
# - OSM_MAX_ACTIVE_TIS_PER_DAGRUN: mapped per-table task concurrency cap
#
# Batch knobs (Option A):
# - OSM_STRATEGY: "pure_parallel" | "batch"
# - OSM_BATCH_SIZE: number of tables per batch (e.g., 2,5,10,20)
# - OSM_MAX_ACTIVE_BATCHES: how many batch tasks can run concurrently (e.g., 2,4,8,10)
#
# Shared resilience:
# - OSM_TASK_RETRIES / OSM_RETRY_DELAY_MINUTES

def _env_int(name: str, default: str) -> int:
    value = os.getenv(name, default)
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"Environment variable {name} must be an integer, got: {value!r}") from exc
    return parsed


def load_runtime_settings() -> dict:
    """
    Load and validate runtime strategy settings from environment.

    Important: because Docker .env values are injected when containers start, any change to
    these settings still requires recreating/restarting the relevant Airflow containers.
    """
    strategy = os.getenv("OSM_STRATEGY", "pure_parallel").strip().lower()
    if strategy not in {"pure_parallel", "batch"}:
        raise ValueError(
            f"Unsupported OSM_STRATEGY={strategy!r}. Allowed values: 'pure_parallel', 'batch'."
        )

    settings = {
        "strategy": strategy,
        "max_active_tis_per_dagrun": _env_int("OSM_MAX_ACTIVE_TIS_PER_DAGRUN", "10"),
        "task_retries": _env_int("OSM_TASK_RETRIES", "3"),
        "retry_delay_minutes": _env_int("OSM_RETRY_DELAY_MINUTES", "2"),
        "batch_size": _env_int("OSM_BATCH_SIZE", "5"),
        "max_active_batches": _env_int("OSM_MAX_ACTIVE_BATCHES", "4"),
    }

    if settings["max_active_tis_per_dagrun"] <= 0:
        raise ValueError("OSM_MAX_ACTIVE_TIS_PER_DAGRUN must be > 0")
    if settings["task_retries"] < 0:
        raise ValueError("OSM_TASK_RETRIES must be >= 0")
    if settings["retry_delay_minutes"] < 0:
        raise ValueError("OSM_RETRY_DELAY_MINUTES must be >= 0")
    if settings["batch_size"] <= 0:
        raise ValueError("OSM_BATCH_SIZE must be > 0")
    if settings["max_active_batches"] <= 0:
        raise ValueError("OSM_MAX_ACTIVE_BATCHES must be > 0")

    return settings




RUNTIME_SETTINGS = load_runtime_settings()

OSM_MAX_ACTIVE_TIS_PER_DAGRUN = RUNTIME_SETTINGS["max_active_tis_per_dagrun"]
OSM_TASK_RETRIES = RUNTIME_SETTINGS["task_retries"]
OSM_RETRY_DELAY_MINUTES = RUNTIME_SETTINGS["retry_delay_minutes"]
OSM_STRATEGY = RUNTIME_SETTINGS["strategy"]
OSM_BATCH_SIZE = RUNTIME_SETTINGS["batch_size"]
OSM_MAX_ACTIVE_BATCHES = RUNTIME_SETTINGS["max_active_batches"]

AIRFLOW_CONN_ID = "osm_postgres"  # matches AIRFLOW_CONN_OSM_POSTGRES in airflow/.env

@dag(
    dag_id="core_osm_table_generator",
    start_date=days_ago(1),
    schedule=None,  # manual trigger for MVP
    catchup=False,
    tags=["osm", "berlin", "airflow"],
)
def core_osm_table_generator():
    @task
    def setup_db() -> str:
        """
        Run-once per DAG run:
        - ensure schema exists
        - ensure PostGIS extension exists
        - ensure metadata table exists
        """
        hook = PostgresHook(postgres_conn_id=AIRFLOW_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};")
        ensure_postgis(cur)
        ensure_metadata_table(cur)
        conn.commit()

        cur.close()
        conn.close()
        return "ok"

    @task
    def get_runtime_settings() -> dict:
        """Expose validated runtime strategy settings to downstream tasks."""
        return RUNTIME_SETTINGS.copy()

    @task.branch
    def choose_strategy(settings: dict) -> str:
        strategy = settings["strategy"]
        if strategy == "batch":
            return "start_batch"
        return "start_pure_parallel"

    @task(
        retries=OSM_TASK_RETRIES,
        retry_delay=timedelta(minutes=OSM_RETRY_DELAY_MINUTES),
        retry_exponential_backoff=True,
        max_active_tis_per_dagrun=OSM_MAX_ACTIVE_TIS_PER_DAGRUN,
        map_index_template="{{ table_label }}",
    )
    def process_table(settings: dict, table_cfg: dict) -> dict:
        """
        One mapped task per table config from osm_tables.json.

        Runtime additions:
        - per-table timing + attempts
        - bounded concurrency for mapped tasks
        - retries with exponential backoff
        """
        ctx = get_current_context()
        run_dt = ctx["logical_date"]  # constant per DAG run
        ti = ctx["ti"]
        map_index = ti.map_index
        attempts = ti.try_number

        table_name = table_cfg.get("table_name", "unknown_table")
        ctx["table_label"] = table_name
        t0 = time.monotonic()

        hook = PostgresHook(postgres_conn_id=AIRFLOW_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        result = process_one_table(conn, cur, run_dt, table_cfg)

        cur.close()
        conn.close()

        duration_sec = time.monotonic() - t0

        result.update(
            {
                "run_ts": str(run_dt),
                "table_name": table_name,
                "map_index": map_index,
                "attempts": attempts,
                "duration_sec": float(duration_sec),
            }
        )

        logger.info(
            "RUN(pure_parallel): table=%s map_index=%s attempts=%s duration_sec=%.2f status=%s",
            table_name,
            map_index,
            attempts,
            duration_sec,
            result.get("status"),
        )
        return result

    @task
    def create_batches(settings: dict, tables: list[dict]) -> list[list[dict]]:
        """
        Option A batching:
        Split table configs into consecutive chunks of batch_size.
        """
        batch_size = int(settings["batch_size"])
        if batch_size <= 0:
            raise ValueError("OSM_BATCH_SIZE must be > 0")
        batches: list[list[dict]] = []
        for i in range(0, len(tables), batch_size):
            batches.append(tables[i : i + batch_size])
        return batches

    @task(
        retries=OSM_TASK_RETRIES,
        retry_delay=timedelta(minutes=OSM_RETRY_DELAY_MINUTES),
        retry_exponential_backoff=True,
        # This caps how many *batch tasks* can run concurrently (parallel batches)
        max_active_tis_per_dagrun=OSM_MAX_ACTIVE_BATCHES,
        map_index_template="{{ batch_label }}",
    )
    def process_batch(settings: dict, batch: list[dict]) -> list[dict]:
        """
        Process a batch sequentially (Option A):
        - this task is mapped over batches
        - each batch task loops tables sequentially
        - returns list[dict] (one dict per table) so we keep per-table visibility
        """
        ctx = get_current_context()
        run_dt = ctx["logical_date"]
        ti = ctx["ti"]
        batch_index = ti.map_index
        batch_attempts = ti.try_number  # attempts for the batch task

        batch_names = [t.get("table_name", "unknown_table") for t in batch]
        if len(batch_names) == 1:
            ctx["batch_label"] = batch_names[0]
        else:
            ctx["batch_label"] = f"{batch_names[0]}__to__{batch_names[-1]}"

        results: list[dict] = []

        # One hook/conn per table
        for i, table_cfg in enumerate(batch):
            table_name = table_cfg.get("table_name", "unknown_table")
            t0 = time.monotonic()

            hook = PostgresHook(postgres_conn_id=AIRFLOW_CONN_ID)
            conn = hook.get_conn()
            cur = conn.cursor()
            try:
                r = process_one_table(conn, cur, run_dt, table_cfg)
            finally:
                try:
                    cur.close()
                except Exception:
                    pass
                try:
                    conn.close()
                except Exception:
                    pass

            duration_sec = time.monotonic() - t0

            r.update(
                {
                    "run_ts": str(run_dt),
                    "table_name": table_name,
                    "batch_index": batch_index,
                    "table_index_in_batch": i,
                    "attempts": batch_attempts,  # batch retries apply to all tables in this batch run
                    "duration_sec": float(duration_sec),
                }
            )
            results.append(r)

            logger.info(
                "RUN(batch): batch_index=%s table=%s idx_in_batch=%s attempts=%s duration_sec=%.2f status=%s",
                batch_index,
                table_name,
                i,
                batch_attempts,
                duration_sec,
                r.get("status"),
            )

        return results

    setup = setup_db()
    settings = get_runtime_settings()
    strategy_choice = choose_strategy(settings)

    start_pure = EmptyOperator(task_id="start_pure_parallel")
    start_batch = EmptyOperator(task_id="start_batch")

    setup >> settings >> strategy_choice
    strategy_choice >> start_pure
    strategy_choice >> start_batch

    mapped = process_table.partial(
        settings=settings,
    ).expand(table_cfg=osm_config["tables"])
    start_pure >> mapped

    batches = create_batches(settings=settings, tables=osm_config["tables"])
    start_batch >> batches

    batch_mapped = process_batch.partial(
        settings=settings,
    ).expand(batch=batches)
    batches >> batch_mapped

# This variable is what Airflow discovers.
dag = core_osm_table_generator()