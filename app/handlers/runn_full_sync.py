# app/handlers/runn_full_sync.py
from __future__ import annotations

import os
import time
import uuid
import logging
from datetime import date, datetime, timedelta
from typing import Dict, Any, List, Optional

import requests
from requests import Response
from google.cloud import bigquery

# =========================
# Config de entorno (no cambies nombres)
# =========================
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

# =========================
# Sesión HTTP con headers
# =========================
SESSION = requests.Session()
SESSION.headers.update(
    {
        "accept": "application/json",
        "accept-version": RUNN_API_VERSION,
        "authorization": f"Bearer {RUNN_API_TOKEN}",
        "user-agent": "charthop-webhook/runn-full-sync",
    }
)

# =========================
# Catálogo de recursos Runn
# =========================
CONFIG: Dict[str, Dict[str, Any]] = {
    # Hechos
    "actuals": {
        "path": "actuals/",
        "pk": "id",
        "ts": "updatedAt",
        "supports_modified": True,
        "partition_field": "date",
    },
    "time-entries": {
        "path": "time-entries/",
        "pk": "id",
        "ts": "updatedAt",
        "supports_modified": True,
        "partition_field": "date",
    },
    "assignments": {
        "path": "assignments/",
        "pk": "id",
        "ts": "updatedAt",
        "supports_modified": True,
        "partition_field": None,
    },
    # Time off families
    "time-offs/leave": {
        "path": "time-offs/leave/",
        "pk": "id",
        "ts": "updatedAt",
        "supports_modified": True,
        "partition_field": "startDate",
    },
    "time-offs/rostered": {
        "path": "time-offs/rostered/",
        "pk": "id",
        "ts": "updatedAt",
        "supports_modified": True,
        "partition_field": "date",
    },
    "time-offs/holidays": {
        "path": "time-offs/holidays/",
        "pk": "id",
        "ts": "updatedAt",
        "supports_modified": True,
        "partition_field": "date",
    },
    # Dimensiones
    "people": {
        "path": "people/",
        "pk": "id",
        "ts": "updatedAt",
        "supports_modified": True,
        "partition_field": None,
    },
    "projects": {
        "path": "projects/",
        "pk": "id",
        "ts": "updatedAt",
        "supports_modified": True,
        "partition_field": None,
    },
    "clients": {
        "path": "clients/",
        "pk": "id",
        "ts": "updatedAt",
        "supports_modified": True,
        "partition_field": None,
    },
    "roles": {
        "path": "roles/",
        "pk": "id",
        "ts": "updatedAt",  # hay tenants sin updatedAt → el MERGE lo detecta y hace upsert plano
        "supports_modified": False,
        "partition_field": None,
    },
    "teams": {
        "path": "teams/",
        "pk": "id",
        "ts": "updatedAt",
        "supports_modified": False,
        "partition_field": None,
    },
    "workstreams": {
        "path": "workstreams/",
        "pk": "id",
        "ts": "updatedAt",
        "supports_modified": True,
        "partition_field": None,
    },
    # Rate cards y project rates
    "rate-cards": {
        "path": "rate-cards/",
        "pk": "id",
        "ts": "updatedAt",
        "supports_modified": True,
        "partition_field": None,
    },
    "project-rates": {
        "path": "project-rates/",
        "pk": "id",
        "ts": "updatedAt",
        "supports_modified": True,
        "partition_field": None,
    },
}

# =========================
# HTTP helpers con reintentos para 429/5xx
# =========================
def _should_retry_status(status: int) -> bool:
    if status == 429:
        return True
    if 500 <= status < 600:
        return True
    return False


def _sleep_backoff(attempt: int, retry_after: Optional[str | int]) -> None:
    if retry_after:
        try:
            wait = int(retry_after)
            time.sleep(min(wait, 60))
            return
        except Exception:
            pass
    # Exponencial simple: 0.5,1,2,4,8,16 (máximo 30s)
    wait = min(0.5 * (2 ** attempt), 30.0)
    time.sleep(wait)


def _fetch_page(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{RUNN_API}/{path}"
    attempt = 0
    while True:
        resp: Response = SESSION.get(url, params=params, timeout=HTTP_TIMEOUT)
        if resp.status_code < 400:
            return resp.json()
        if not _should_retry_status(resp.status_code):
            raise RuntimeError(f"GET {path} -> {resp.status_code}: {resp.text[:400]}")
        attempt += 1
        _sleep_backoff(attempt, resp.headers.get("Retry-After"))


def _paged_collect(resource: str, window_days: int) -> List[Dict[str, Any]]:
    cfg = CONFIG[resource]
    values: List[Dict[str, Any]] = []
    cursor: Optional[str] = None

    today = date.today()
    min_date = (today - timedelta(days=window_days)).isoformat()
    max_date = today.isoformat()

    params: Dict[str, Any] = {"limit": 200}

    # Ventanas por fecha para hechos
    if resource in ("actuals", "time-entries"):
        params.update({"minDate": min_date, "maxDate": max_date})

    if resource.startswith("time-offs/"):
        params.update({"minDate": min_date, "maxDate": max_date})

    # Incremental por updatedAt si lo soporta el endpoint
    if cfg["supports_modified"]:
        modified_after = (
            datetime.utcnow() - timedelta(days=window_days)
        ).replace(microsecond=0).isoformat() + "Z"
        params["modifiedAfter"] = modified_after

    while True:
        page_params = dict(params)
        if cursor:
            page_params["cursor"] = cursor
        data = _fetch_page(cfg["path"], page_params)
        page = data.get("values") or data.get("items") or []
        if not isinstance(page, list):
            raise RuntimeError(f"Respuesta inesperada en {resource}: {type(page)}")
        values.extend(page)
        cursor = data.get("nextCursor")
        log.info("GET /%s page len=%s cursor=%s", cfg["path"].rstrip("/"), len(page), bool(cursor))
        if not cursor:
            break
        time.sleep(0.2)  # cortesía para la API
    return values


# =========================
# BigQuery helpers
# =========================
def _table_name(resource: str) -> str:
    safe = resource.replace("/", "_").replace("-", "_")
    return f"{BQ_PROJECT}.{BQ_DATASET}.runn_{safe}"


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
        pass  # no existe

    # Crear target copiando el staging (incluye autodetect)
    copy_job = client.copy_table(staging_fqn, target_fqn, location=BQ_LOCATION)
    copy_job.result()

    # Añadir partición si aplica (solo cuando es tabla nueva)
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

    target = _table_name(resource)
    staging = f"{BQ_PROJECT}.{BQ_DATASET}._runn_{resource.replace('/', '_').replace('-', '_')}_stg_{uuid.uuid4().hex[:8]}"

    # Cargar staging con autodetección
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

    # Asegurar target (si no existe, lo creo copiando staging y agrego partición si aplica)
    _ensure_table_from_staging_if_missing(client, target, staging, cfg["partition_field"])

    # Resolver esquema para MERGE
    target_schema = client.get_table(target).schema
    schema_map = {f.name: f for f in target_schema}
    staging_fields = {f.name for f in client.get_table(staging).schema}
    columns = [f.name for f in target_schema if f.name in staging_fields]

    if not columns:
        # nada compatible; dejo la staging creada para inspección
        return {"inserted": 0, "merged": 0, "table": target, "note": "no compatible columns"}

    pk = cfg["pk"]
    ts = cfg["ts"]

    assignments = ",\n        ".join(
        f"T.{col} = SAFE_CAST(S.{col} AS {schema_map[col].field_type})" for col in columns
    )
    insert_columns = ", ".join(columns)
    insert_values = ", ".join(
        f"SAFE_CAST(S.{col} AS {schema_map[col].field_type})" for col in columns
    )

    # ¿Tenemos columna de timestamp en ambos lados (staging y target)?
    has_ts = ts in staging_fields and ts in schema_map

    if has_ts:
        # MERGE con comparación por timestamp (solo actualiza si S.ts > T.ts)
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
        # MERGE sin timestamp: cualquier match se actualiza
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

    # Limpio staging
    client.delete_table(staging, not_found_ok=True)

    return {
        "inserted": job.num_dml_inserted_rows or 0,
        "merged": job.num_dml_updated_rows or 0,
        "table": target,
    }


def run_full_sync(window_days: Optional[int] = None, targets: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Ejecuta la exportación completa. `targets` te permite filtrar recursos.
    """
    client = bigquery.Client(project=BQ_PROJECT, location=BQ_LOCATION)

    window = int(window_days or WINDOW_DAYS)
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
    # Ejecutable como: python -m app.handlers.runn_full_sync
    result = run_full_sync()
    print(result)
