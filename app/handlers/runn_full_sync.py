# handlers/runn_full_sync.py
import os, time, uuid
from datetime import date, datetime, timedelta
from typing import Dict, Any, List, Optional
import requests
from google.cloud import bigquery

RUNN_API = os.environ.get("RUNN_API", "https://api.runn.io")
RUNN_API_VERSION = os.environ.get("RUNN_API_VERSION", "1.0.0")
RUNN_API_TOKEN = os.environ["RUNN_API_TOKEN"]

BQ_PROJECT = os.environ["BQ_PROJECT"]
BQ_DATASET = os.environ["BQ_DATASET"]
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")

SESSION = requests.Session()
SESSION.headers.update({
    "accept": "application/json",
    "accept-version": RUNN_API_VERSION,
    "authorization": f"Bearer {RUNN_API_TOKEN}",
})

# Configuración por recurso
CONFIG = {
    # hechos
    "actuals":       {"path": "actuals/",       "pk": "id", "ts": "updatedAt", "supports_modified": True,  "partition_field": "date"},
    "assignments":   {"path": "assignments/",   "pk": "id", "ts": "updatedAt", "supports_modified": True,  "partition_field": None},
    "time-offs":     {"path": "time-offs/",     "pk": "id", "ts": "updatedAt", "supports_modified": True,  "partition_field": "startDate"},
    # dimensiones
    "people":        {"path": "people/",        "pk": "id", "ts": "updatedAt", "supports_modified": True,  "partition_field": None},
    "projects":      {"path": "projects/",      "pk": "id", "ts": "updatedAt", "supports_modified": True,  "partition_field": None},
    "roles":         {"path": "roles/",         "pk": "id", "ts": "updatedAt", "supports_modified": False, "partition_field": None},
    "teams":         {"path": "teams/",         "pk": "id", "ts": "updatedAt", "supports_modified": False, "partition_field": None},
    "clients":       {"path": "clients/",       "pk": "id", "ts": "updatedAt", "supports_modified": True,  "partition_field": None},
    "workstreams":   {"path": "workstreams/",   "pk": "id", "ts": "updatedAt", "supports_modified": True,  "partition_field": None},
    # opcionales
    "rate-cards":    {"path": "rate-cards/",    "pk": "id", "ts": "updatedAt", "supports_modified": True,  "partition_field": None},
}

def _fetch(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{RUNN_API.rstrip('/')}/{path}"
    resp = SESSION.get(url, params=params, timeout=45)
    # Si ves 404 aquí es casi siempre path/params mal formados
    resp.raise_for_status()
    return resp.json()

def _paged_collect(resource: str, window_days: int, max_days_back: Optional[int] = None) -> List[Dict[str, Any]]:
    cfg = CONFIG[resource]
    values: List[Dict[str, Any]] = []
    cursor = None

    today = date.today()
    min_date = (today - timedelta(days=window_days)).isoformat()
    max_date = today.isoformat()

    # Params base
    params: Dict[str, Any] = {"limit": 200}

    # Para hechos tipo actuals/time-offs uso minDate/maxDate
    if resource in ("actuals", "time-offs"):
        params.update({"minDate": min_date, "maxDate": max_date})

    # Para incrementales por updatedAt cuando el endpoint lo permite
    if cfg["supports_modified"]:
        # Ventana de modificación reciente por seguridad
        modified_after = (datetime.utcnow() - timedelta(days=window_days)).replace(microsecond=0).isoformat() + "Z"
        params["modifiedAfter"] = modified_after

    while True:
        if cursor:
            params["cursor"] = cursor
        data = _fetch(cfg["path"], params)
        page = data.get("values") or data.get("items") or []
        values.extend(page)
        cursor = data.get("nextCursor")
        if not cursor:
            break
        time.sleep(0.2)
    return values

def _table_name(resource: str) -> str:
    safe = resource.replace("-", "_")
    return f"{BQ_PROJECT}.{BQ_DATASET}.runn_{safe}"

def _ensure_table(client: bigquery.Client, table_fqn: str, sample: Dict[str, Any], partition_field: Optional[str]):
    # Detección simple de tipos
    def bq_type(v):
        if isinstance(v, bool): return "BOOL"
        if isinstance(v, int): return "INT64"
        if isinstance(v, float): return "FLOAT64"
        # timestamps/fechas los infiere el load si vienen como string ISO
        return "STRING"

    schema = []
    for k, v in sample.items():
        # listas/objetos anidados => JSON string para simplificar (se puede refinar a RECORD si lo necesitas)
        if isinstance(v, (list, dict)) or v is None:
            schema.append(bigquery.SchemaField(k, "STRING"))
        else:
            schema.append(bigquery.SchemaField(k, bq_type(v)))

    table = bigquery.Table(table_fqn, schema=schema)
    if partition_field and partition_field in sample:
        table.time_partitioning = bigquery.TimePartitioning(field=partition_field)

    try:
        client.get_table(table_fqn)
    except Exception:
        client.create_table(table)

def _merge_upsert(client: bigquery.Client, resource: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {"inserted": 0, "merged": 0}

    cfg = CONFIG[resource]
    table = _table_name(resource)
    staging = f"{BQ_PROJECT}.{BQ_DATASET}._runn_{resource.replace('-', '_')}_stg_{uuid.uuid4().hex[:8]}"

    _ensure_table(client, table, rows[0], cfg["partition_field"])

    job = client.load_table_from_json(
        rows, staging,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
        ),
        location=BQ_LOCATION
    )
    job.result()

    target_schema = client.get_table(table).schema
    staging_schema = {field.name for field in client.get_table(staging).schema}
    columns = [field.name for field in target_schema if field.name in staging_schema]

    if not columns:
        return {"inserted": 0, "merged": 0, "table": table}

    pk = cfg["pk"]
    ts = cfg["ts"]

    assignments = ",\n        ".join(f"T.{col} = S.{col}" for col in columns)
    insert_columns = ", ".join(columns)
    insert_values = ", ".join(f"S.{col}" for col in columns)

    # Si no hay updatedAt en el recurso, comparamos contra NULL => siempre inserta/actualiza
    merge_sql = f"""
    MERGE `{table}` T
    USING `{staging}` S
    ON CAST(T.{pk} AS STRING) = CAST(S.{pk} AS STRING)
    WHEN MATCHED AND (
        SAFE.TIMESTAMP(S.{ts}) > SAFE.TIMESTAMP(T.{ts})
        OR T.{ts} IS NULL OR S.{ts} IS NULL
    ) THEN UPDATE SET
        {assignments}
    WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
    """
    q = client.query(merge_sql, location=BQ_LOCATION); q.result()
    try:
        client.delete_table(staging, not_found_ok=True)
    except Exception:
        pass
    return {"inserted": len(rows), "merged": q.num_dml_affected_rows or 0, "table": table}

def export_handler(
    window_days: int = 120,
    entities: Optional[str] = None,
    **_
):
    """
    Sincroniza múltiples recursos de Runn a BigQuery.
    - window_days: ventana para minDate/maxDate y modifiedAfter
    - entities: CSV de recursos (por ejemplo: "actuals,assignments,people").
                Si no se envía, uso un set razonable.
    """
    default_entities = [
        "actuals", "assignments", "time-offs",
        "people", "projects", "roles", "teams", "clients", "workstreams"
    ]
    targets = [e.strip() for e in (entities.split(",") if entities else default_entities)]
    client = bigquery.Client(project=BQ_PROJECT)

    out = {"ok": True, "window_days": window_days, "results": []}
    for res in targets:
        if res not in CONFIG:
            out["results"].append({"resource": res, "ok": False, "error": "unsupported"})
            continue
        data = _paged_collect(res, window_days)
        stat = _merge_upsert(client, res, data)
        out["results"].append({
            "resource": res,
            "ok": True,
            "fetched": len(data),
            **stat
        })
    return out
