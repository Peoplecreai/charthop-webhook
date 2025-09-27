# app/services/runn_export_all.py
# Exporta "todo" lo relevante de Runn v1 → BigQuery
# Sin SQL, con paginación por cursor, backoff y cargas idempotentes por ventana.
import os
import time
import logging
from datetime import date, timedelta
from typing import Dict, Iterable, List, Optional

import requests
import pandas as pd
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception, before_sleep_log
from google.cloud import bigquery

# === Config a partir de TUS variables/secretos ===
RUNN_API_BASE     = os.getenv("RUNN_API", "https://api.runn.io").rstrip("/")
RUNN_API_VERSION  = os.getenv("RUNN_API_VERSION", "1.0.0")
RUNN_API_TOKEN    = os.environ["RUNN_API_TOKEN"]  # secreto
HTTP_TIMEOUT      = int(os.getenv("HTTP_TIMEOUT", "30"))

BQ_PROJECT        = os.environ["BQ_PROJECT"]
BQ_DATASET        = os.environ["BQ_DATASET"]
BQ_LOCATION       = os.getenv("BQ_LOCATION", "US")

# Ventana por defecto para hechos (puedes sobreescribir con env)
WINDOW_DAYS       = int(os.getenv("WINDOW_DAYS", "180"))
# Concurrencia suave para subrecursos por proyecto
MAX_WORKERS       = int(os.getenv("MAX_WORKERS", "4"))
# Tamaño de página sugerido por API v1
PAGE_LIMIT        = int(os.getenv("RUNN_LIMIT", "200"))

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("runn-export")

HEADERS = {
    "Authorization": f"Bearer {RUNN_API_TOKEN}",
    "Accept-Version": RUNN_API_VERSION,
    "accept": "application/json",
}

# Cliente BQ
bq = bigquery.Client(project=BQ_PROJECT)

# ===== Utilidades HTTP / paginación =====

class RunnRetry(Exception):
    pass

def _retryable(exc: Exception) -> bool:
    if isinstance(exc, requests.HTTPError):
        s = exc.response.status_code
        return s in (408, 409, 425, 429, 500, 502, 503, 504)
    return isinstance(exc, (requests.ConnectionError, requests.Timeout))

def _url(path: str) -> str:
    return f"{RUNN_API_BASE}{path}"

@retry(wait=wait_exponential(multiplier=1, min=1, max=30),
       stop=stop_after_attempt(7),
       retry=retry_if_exception(_retryable),
       before_sleep=before_sleep_log(log, logging.WARNING))
def _get(path: str, params: Optional[Dict] = None) -> Dict:
    q = dict(params or {})
    if "limit" not in q:
        q["limit"] = PAGE_LIMIT
    r = requests.get(_url(path), headers=HEADERS, params=q, timeout=HTTP_TIMEOUT)
    if r.status_code == 429:
        # Respeta rate limit si lo envían
        retry_after = int(r.headers.get("retry-after", "5"))
        time.sleep(max(5, retry_after))
    r.raise_for_status()
    return r.json()

def fetch_all(path: str, params: Optional[Dict] = None) -> List[Dict]:
    q = dict(params or {})
    out: List[Dict] = []
    cursor = None
    while True:
        if cursor:
            q["cursor"] = cursor
        page = _get(path, q)
        vals = page.get("values", page if isinstance(page, list) else [])
        out.extend(vals)
        cursor = page.get("nextCursor")
        if not cursor:
            break
    log.info("GET %s total=%s", path, len(out))
    return out

# ===== Helpers de normalización y carga =====

def _normalize(df: pd.DataFrame, required: Iterable[str] = ()) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame([{k: None for k in required}]) if required else pd.DataFrame()
    for c in required:
        if c not in df.columns:
            df[c] = None
    return df

def _to_df(rows: List[Dict]) -> pd.DataFrame:
    return pd.json_normalize(rows) if rows else pd.DataFrame()

def _ensure_dataset():
    ds_ref = bigquery.Dataset(f"{BQ_PROJECT}.{BQ_DATASET}")
    try:
        bq.get_dataset(ds_ref)
    except Exception:
        ds_ref.location = BQ_LOCATION
        bq.create_dataset(ds_ref, exists_ok=True)

def load_df(df: pd.DataFrame, table: str, write_disposition="WRITE_TRUNCATE", partition_field: Optional[str] = None):
    _ensure_dataset()
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{table}"
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
    # Si particionamos por fecha, aseguremos tipo datetime
    if partition_field and partition_field in df.columns and df[partition_field].dtype == "O":
        try:
            df[partition_field] = pd.to_datetime(df[partition_field])
        except Exception:
            pass
    # Crea tabla particionada si no existe
    if partition_field:
        try:
            bq.get_table(table_id)
        except Exception:
            schema = []
            for col, dtype in zip(df.columns, df.dtypes):
                if dtype.kind in ("i","u"):
                    typ = "INT64"
                elif dtype.kind == "f":
                    typ = "FLOAT64"
                elif dtype.kind == "b":
                    typ = "BOOL"
                elif "datetime64" in str(dtype):
                    typ = "TIMESTAMP"
                else:
                    typ = "STRING"
                schema.append(bigquery.SchemaField(col, typ))
            t = bigquery.Table(table_id, schema=schema)
            t.time_partitioning = bigquery.TimePartitioning(field=partition_field)
            bq.create_table(t)
    job = bq.load_table_from_dataframe(df, table_id, location=BQ_LOCATION, job_config=job_config)
    job.result()
    log.info("BQ load %s rows=%s", table, len(df))

def _window(days: int):
    since = (date.today() - timedelta(days=days)).isoformat()
    until = date.today().isoformat()
    return since, until

# ===== Exportadores =====

def export_dimensions():
    # Catálogos principales
    dims = {
        "runn_clients":        ("/clients", {}),
        "runn_people":         ("/people", {}),
        "runn_placeholders":   ("/placeholders", {}),
        "runn_roles":          ("/roles", {}),
        "runn_teams":          ("/teams", {}),
        "runn_skills":         ("/skills", {}),
        "runn_people_tags":    ("/people-tags", {}),
        "runn_project_tags":   ("/project-tags", {}),
        "runn_rate_cards":     ("/rate-cards", {}),
        "runn_holiday_groups": ("/holiday-groups", {}),
        "runn_custom_fields":  ("/custom-fields", {}),
        "runn_users":          ("/users", {}),
        "runn_projects":       ("/projects", {}),
    }
    for table, (path, params) in dims.items():
        rows = fetch_all(path, params)
        df = _to_df(rows)
        load_df(df, table)

def export_time_offs(days: int):
    since, until = _window(days)
    leave     = fetch_all("/time-offs/leave",        {"startDate": since, "endDate": until})
    rostered  = fetch_all("/time-offs/rostered-off", {"startDate": since, "endDate": until})
    holidays  = fetch_all("/time-offs/holidays",     {"startDate": since, "endDate": until})
    df_leave  = _normalize(_to_df(leave),    ("personId","startDate","endDate","type"))
    df_rost   = _normalize(_to_df(rostered), ("personId","date","type"))
    df_holid  = _normalize(_to_df(holidays), ("personId","date","type","holidayGroupId"))
    load_df(df_leave, "runn_timeoffs_leave")
    load_df(df_rost,  "runn_timeoffs_rostered")
    load_df(df_holid, "runn_timeoffs_holidays")

def export_assignments(days: int):
    since, until = _window(days)
    rows = fetch_all("/assignments", {"startDate": since, "endDate": until})
    df   = _normalize(_to_df(rows), ("personId","projectId","startDate","endDate","allocation","isBillable"))
    load_df(df, "runn_assignments")

def export_actuals(days: int):
    since, until = _window(days)
    rows = fetch_all("/actuals", {"minDate": since, "maxDate": until})
    df   = _normalize(_to_df(rows), ("date","personId","projectId","roleId","hours","isBillable"))
    load_df(df, "runn_actuals", partition_field="date")

def export_project_subresources(days: int):
    # Para cruzar fácilmente por proyecto
    since, until = _window(days)
    prows = fetch_all("/projects")
    pids = [r["id"] for r in prows if isinstance(r, dict) and "id" in r]
    log.info("Proyectos: %s", len(pids))

    agg = {
        "runn_phases": [],
        "runn_milestones": [],
        "runn_project_rates": [],
        "runn_notes": [],
        "runn_people_on_project": [],
        "runn_project_assignments": [],
        "runn_project_actuals": [],
    }

    # Concurrencia ligera
    from concurrent.futures import ThreadPoolExecutor, as_completed
    def pull(pid: int) -> Dict[str, pd.DataFrame]:
        out: Dict[str, pd.DataFrame] = {}
        def df_plus(rows):
            df = _to_df(rows)
            if not df.empty:
                df["projectId"] = pid
            return df

        out["runn_phases"] = df_plus(fetch_all(f"/projects/{pid}/phases"))
        out["runn_milestones"] = df_plus(fetch_all(f"/projects/{pid}/milestones", {"startDate": since, "endDate": until}))
        out["runn_project_rates"] = df_plus(fetch_all(f"/projects/{pid}/project-rates"))
        out["runn_notes"] = df_plus(fetch_all(f"/projects/{pid}/notes"))
        out["runn_people_on_project"] = df_plus(fetch_all(f"/projects/{pid}/people"))

        pasg = df_plus(fetch_all(f"/projects/{pid}/assignments", {"startDate": since, "endDate": until}))
        if not pasg.empty:
            pasg = _normalize(pasg, ("personId","startDate","endDate","allocation","isBillable","projectId"))
        out["runn_project_assignments"] = pasg

        pact = df_plus(fetch_all(f"/projects/{pid}/actuals", {"minDate": since, "maxDate": until}))
        if not pact.empty:
            pact = _normalize(pact, ("date","personId","roleId","hours","isBillable","projectId"))
        out["runn_project_actuals"] = pact
        return out

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(pull, pid): pid for pid in pids}
        for fut in as_completed(futures):
            res = fut.result()
            for k, df in res.items():
                if df is not None and not df.empty:
                    agg[k].append(df)

    for table, chunks in agg.items():
        df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
        part = "date" if table == "runn_project_actuals" and "date" in df.columns else None
        load_df(df, table, partition_field=part)

def export_holidays_detail():
    groups = fetch_all("/holiday-groups")
    frames = []
    for g in groups:
        gid = g.get("id")
        if gid is None:
            continue
        hs = fetch_all(f"/holiday-groups/{gid}/holidays")
        df = _to_df(hs)
        if not df.empty:
            df["holidayGroupId"] = gid
            frames.append(df)
    df_all = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    load_df(df_all, "runn_holidays")

def run_full_sync(days: int = WINDOW_DAYS):
    export_dimensions()
    export_time_offs(days)
    export_assignments(days)
    export_actuals(days)
    export_project_subresources(days)
    export_holidays_detail()
