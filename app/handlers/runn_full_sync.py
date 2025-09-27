# app/handlers/runn_full_sync.py
from __future__ import annotations

import os
import time
import uuid
import json
import hashlib
import logging
from datetime import date, datetime, timedelta
from typing import Dict, Any, List, Optional

import requests
from requests import Response
from google.cloud import bigquery

# ===========
# Entorno
# ===========
RUNN_API = os.environ.get("RUNN_API", "https://api.runn.io").rstrip("/")
RUNN_API_VERSION = os.environ.get("RUNN_API_VERSION", "1.0.0")
RUNN_API_TOKEN = os.environ["RUNN_API_TOKEN"]

BQ_PROJECT = os.environ["BQ_PROJECT"]
BQ_DATASET = os.environ["BQ_DATASET"]
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")

HTTP_TIMEOUT = float(os.environ.get("HTTP_TIMEOUT", "45"))
WINDOW_DAYS = int(os.environ.get("WINDOW_DAYS", "120"))

log = logging.getLogger("runn-full-sync")
logging.basicConfig(level=logging.INFO)

# ===========
# HTTP session
# ===========
SESSION = requests.Session()
SESSION.headers.update(
    {
        "accept": "application/json",
        "accept-version": RUNN_API_VERSION,
        "authorization": f"Bearer {RUNN_API_TOKEN}",
        "user-agent": "charthop-webhook/runn-full-sync",
    }
)

# ===========
# Catálogo de recursos
#   path: ruta relativa (con / final)
#   pk:   campo PK en la respuesta; si None, generamos pk sintético
#   ts:   campo de “última actualización” si existe; si no, comparamos sin timestamp
#   supports_modified: si acepta ?modifiedAfter
#   partition_field:   para particiones en BQ cuando aplica
#   single_object:     endpoint devuelve un objeto (no lista)
# ===========
CONFIG: Dict[str, Dict[str, Any]] = {
    # Hechos
    "actuals":            {"path":"actuals/","pk":"id","ts":"updatedAt","supports_modified": True,  "partition_field":"date",       "single_object": False},
    "time-entries":       {"path":"time-entries/","pk":"id","ts":"updatedAt","supports_modified": True,"partition_field":"date",     "single_object": False},
    "assignments":        {"path":"assignments/","pk":"id","ts":"updatedAt","supports_modified": True,"partition_field": None,       "single_object": False},

    # Time Offs
    "time-offs/leave":    {"path":"time-offs/leave/","pk":"id","ts":"updatedAt","supports_modified": True, "partition_field":"startDate","single_object": False},
    "time-offs/rostered": {"path":"time-offs/rostered/","pk":"id","ts":"updatedAt","supports_modified": True,"partition_field":"date","single_object": False},
    "time-offs/holidays": {"path":"time-offs/holidays/","pk":"id","ts":"updatedAt","supports_modified": True,"partition_field":"date","single_object": False},

    # Dimensiones (FULL SCAN por defecto: muchos no aceptan modifiedAfter fiable)
    "people":             {"path":"people/","pk":"id","ts":"updatedAt","supports_modified": False,"partition_field": None,"single_object": False},
    "projects":           {"path":"projects/","pk":"id","ts":"updatedAt","supports_modified": False,"partition_field": None,"single_object": False},
    "clients":            {"path":"clients/","pk":"id","ts":"updatedAt","supports_modified": False,"partition_field": None,"single_object": False},
    "roles":              {"path":"roles/","pk":"id","ts":"updatedAt","supports_modified": False,"partition_field": None,"single_object": False},
    "teams":              {"path":"teams/","pk":"id","ts":"updatedAt","supports_modified": False,"partition_field": None,"single_object": False},
    "workstreams":        {"path":"workstreams/","pk":"id","ts":"updatedAt","supports_modified": False,"partition_field": None,"single_object": False},
    "placeholders":       {"path":"placeholders/","pk":"id","ts":"updatedAt","supports_modified": False,"partition_field": None,"single_object": False},
    "skills":             {"path":"skills/","pk":"id","ts":"updatedAt","supports_modified": False,"partition_field": None,"single_object": False},
    "people-tags":        {"path":"people-tags/","pk":"id","ts":"updatedAt","supports_modified": False,"partition_field": None,"single_object": False},
    "project-tags":       {"path":"project-tags/","pk":"id","ts":"updatedAt","supports_modified": False,"partition_field": None,"single_object": False},
    "holiday-groups":     {"path":"holiday-groups/","pk":"id","ts":"updatedAt","supports_modified": False,"partition_field": None,"single_object": False},
    "contracts":          {"path":"contracts/","pk":"id","ts":"updatedAt","supports_modified": False,"partition_field": None,"single_object": False},
    "custom-fields":      {"path":"custom-fields/","pk":"id","ts":"updatedAt","supports_modified": False,"partition_field": None,"single_object": False},

    # Rate cards
    "rate-cards":         {"path":"rate-cards/","pk":"id","ts":"updatedAt","supports_modified": False,"partition_field": None,"single_object": False},
    "project-rates":      {"path":"project-rates/","pk":"id","ts":"updatedAt","supports_modified": False,"partition_field": None,"single_object": False},

    # Administración (depende de permisos del token)
    "users":              {"path":"users/","pk":"id","ts":"updatedAt","supports_modified": False,"partition_field": None,"single_object": False},
    "invitations":        {"path":"invitations/","pk":"id","ts":"updatedAt","supports_modified": False,"partition_field": None,"single_object": False},

    # “Me” devuelve un objeto
    "me":                 {"path":"me/","pk": None,"ts":"updatedAt","supports_modified": False,"partition_field": None,"single_object": True},

    # Fuera por ahora: activity-log (alto volumen, ventanas especiales), reports/views/utility (no datasets crudos)
    # Si quieres activity-log, avísame y lo activamos con partición por createdAt + ventana diaria.
}

# ===========
# Reintentos 429/5xx
# ===========
def _should_retry_status(status: int) -> bool:
    return status == 429 or (500 <= status < 600)

def _sleep_backoff(attempt: int, retry_after: Optional[str | int]) -> None:
    if retry_after:
        try:
            wait = int(retry_after)
            time.sleep(min(wait, 60))
            return
        except Exception:
            pass
    time.sleep(min(0.5 * (2 ** attempt), 30.0))

def _url(path: str) -> str:
    return f"{RUNN_API}/{path.lstrip('/')}"

def _fetch_page(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    url = _url(path)
    attempt = 0
    while True:
        r: Response = SESSION.get(url, params=params, timeout=HTTP_TIMEOUT)
        if r.status_code < 400:
            return r.json()
        if not _should_retry_status(r.status_code):
            raise RuntimeError(f"GET {path} -> {r.status_code}: {r.text[:400]}")
        attempt += 1
        _sleep_backoff(attempt, r.headers.get("Retry-After"))

# ===========
# Normalización de registros
#   - añade raw JSON
#   - rellena pk sintético si falta
#   - rellena updatedAt con createdAt si no existe
# ===========
def _synthetic_pk(obj: Dict[str, Any]) -> str:
    # hash determinista del objeto “flattened”
    j = json.dumps(obj, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(j.encode("utf-8")).hexdigest()

def _normalize_rows(rows: List[Dict[str, Any]], pk_field: Optional[str], ts_field: Optional[str]) -> List[Dict[str, Any]]:
    norm: List[Dict[str, Any]] = []
    for o in rows:
        rec = dict(o)
        rec["raw"] = o  # conserva payload completo
        if not pk_field or pk_field not in rec or rec.get(pk_field) in (None, ""):
            rec["pk"] = _synthetic_pk(o)
        if ts_field and ts_field not in rec and "createdAt" in rec:
            rec[ts_field] = rec.get("createdAt")
        norm.append(rec)
    return norm

# ===========
# Paginación + fallback sin modifiedAfter
# ===========
def _paged_collect(resource: str, window_days: int) -> List[Dict[str, Any]]:
    cfg = CONFIG[resource]
    today = date.today()
    min_date = (today - timedelta(days=window_days)).isoformat()
    max_date = today.isoformat()

    params: Dict[str, Any] = {"limit": 200}

    # Ventanas por fecha para hechos/time-offs
    if resource in ("actuals", "time-entries"):
        params.update({"minDate": min_date, "maxDate": max_date})
    if resource.startswith("time-offs/"):
        params.update({"minDate": min_date, "maxDate": max_date})

    if cfg["supports_modified"]:
        modified_after = (
            datetime.utcnow() - timedelta(days=window_days)
        ).replace(microsecond=0).isoformat() + "Z"
        params["modifiedAfter"] = modified_after

    # single_object: pido una vez y lo envuelvo en lista
    if cfg.get("single_object"):
        data = _fetch_page(cfg["path"], params={})
        return [data] if isinstance(data, dict) else []

    def collect(p: Dict[str, Any]) -> List[Dict[str, Any]]:
        values: List[Dict[str, Any]] = []
        cursor: Optional[str] = None
        while True:
            page_params = dict(p)
            if cursor:
                page_params["cursor"] = cursor
            data = _fetch_page(cfg["path"], page_params)
            page = data.get("values") or data.get("items") or []
            if not isinstance(page, list):
                raise RuntimeError(f"Respuesta inesperada en {resource}: {type(page)}")
            values.extend(page)
            cursor = data.get("nextCursor")
            log.info("GET /%s page=%s total=%s", cfg["path"].rstrip("/"), len(page), len(values))
            if not cursor:
                break
            time.sleep(0.2)
        return values

    values = collect(params)

    # Fallback: si pediste con modifiedAfter y vino vacío en un recurso que NO es por ventana
    windowed = (resource in ("actuals", "time-entries")) or resource.startswith("time-offs/")
    if not values and ("modifiedAfter" in params) and (not windowed):
        p2 = dict(params); p2.pop("modifiedAfter", None)
        log.warning("Fallback %s sin modifiedAfter (primer intento vacío).", resource)
        values = collect(p2)

    return values

# ===========
# BigQuery helpers
# ===========
def _safe_name(resource: str) -> str:
    return resource.replace("/", "_").replace("-", "_")

def _table_name(resource: str) -> str:
    return f"{BQ_PROJECT}.{BQ_DATASET}.runn_{_safe_name(resource)}"

def _ensure_table_from_staging_if_missing(
    client: bigquery.Client,
    target_fqn: str,
    staging_fqn: str,
    partition_field: Optional[str],
) -> None:
    try:
        client.get_table(target_fqn)
        return
    except Exception:
        pass
    copy_job = client.copy_table(staging_fqn, target_fqn, location=BQ_LOCATION)
    copy_job.result()

    if partition_field:
        tbl = client.get_table(target_fqn)
        if not tbl.time_partitioning:
            from google.cloud.bigquery.table import TimePartitioning, TimePartitioningType
            tbl.time_partitioning = TimePartitioning(
                type_=TimePartitioningType.DAY, field=partition_field
            )
            client.update_table(tbl, ["time_partitioning"])

def _merge_upsert(
    client: bigquery.Client, resource: str, rows: List[Dict[str, Any]]
) -> Dict[str, Any]:
    cfg = CONFIG[resource]
    if not rows:
        return {"inserted": 0, "merged": 0, "table": _table_name(resource)}

    # Normaliza filas (pk sintético si falta; ts desde createdAt si no viene)
    rows = _normalize_rows(rows, pk_field=cfg.get("pk"), ts_field=cfg.get("ts"))

    target = _table_name(resource)
    staging = f"{BQ_PROJECT}.{BQ_DATASET}._runn_{_safe_name(resource)}_stg_{uuid.uuid4().hex[:8]}"

    load_job = client.load_table_from_json(
        rows,
        staging,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
        ),
        location=BQ_LOCATION,
    )
    load_job.result()

    _ensure_table_from_staging_if_missing(client, target, staging, cfg["partition_field"])

    # Resolver esquema/mapeo columnas compatibles
    target_schema = client.get_table(target).schema
    schema_map = {f.name: f for f in target_schema}
    staging_fields = {f.name for f in client.get_table(staging).schema}
    columns = [f.name for f in target_schema if f.name in staging_fields]
    if not columns:
        return {"inserted": 0, "merged": 0, "table": target, "note": "no compatible columns"}

    pk = cfg.get("pk") or "pk"
    ts = cfg.get("ts")

    assignments = ",\n        ".join(
        f"T.{col} = SAFE_CAST(S.{col} AS {schema_map[col].field_type})" for col in columns
    )
    insert_columns = ", ".join(columns)
    insert_values = ", ".join(
        f"SAFE_CAST(S.{col} AS {schema_map[col].field_type})" for col in columns
    )

    has_ts = ts and (ts in staging_fields) and (ts in schema_map)

    if has_ts:
        merge_sql = f"""
        MERGE `{target}` T
        USING `{staging}` S
        ON CAST(T.{pk} AS STRING) = CAST(S.{pk} AS STRING)
        WHEN MATCHED AND (
            (SAFE.TIMESTAMP(S.{ts}) > SAFE.TIMESTAMP(T.{ts}))
            OR T.{ts} IS NULL
            OR S.{ts} IS NULL
        ) THEN
          UPDATE SET
            {assignments}
        WHEN NOT MATCHED THEN
          INSERT ({insert_columns})
          VALUES ({insert_values})
        """
    else:
        merge_sql = f"""
        MERGE `{target}` T
        USING `{staging}` S
        ON CAST(T.{pk} AS STRING) = CAST(S.{pk} AS STRING)
        WHEN MATCHED THEN
          UPDATE SET
            {assignments}
        WHEN NOT MATCHED THEN
          INSERT ({insert_columns})
          VALUES ({insert_values})
        """

    job = client.query(merge_sql, location=BQ_LOCATION)
    job.result()

    client.delete_table(staging, not_found_ok=True)
    return {
        "inserted": job.num_dml_inserted_rows or 0,
        "merged": job.num_dml_updated_rows or 0,
        "table": target,
    }

def run_full_sync(window_days: Optional[int] = None, targets: Optional[List[str]] = None) -> Dict[str, Any]:
    client = bigquery.Client(project=BQ_PROJECT, location=BQ_LOCATION)
    window = int(window_days or WINDOW_DAYS)

    # por defecto: TODO lo listable del catálogo (excepto lo que luego quieras excluir)
    resources = targets or list(CONFIG.keys())

    out: Dict[str, Any] = {"ok": True, "window_days": window, "results": []}
    for res in resources:
        if res not in CONFIG:
            out["results"].append({"resource": res, "ok": False, "error": "unsupported"})
            continue
        log.info("Exportando %s...", res)
        data = _paged_collect(res, window)
        stat = _merge_upsert(client, res, data)
        out["results"].append({"resource": res, "ok": True, "fetched": len(data), **stat})
    return out

if __name__ == "__main__":
    print(run_full_sync())
