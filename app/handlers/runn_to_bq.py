# handlers/runn_to_bq.py
import os, time, math, datetime as dt
import requests
from google.cloud import bigquery

RUNN_API = os.getenv("RUNN_API", "https://api.runn.io")
RUNN_API_VERSION = os.getenv("RUNN_API_VERSION", "1.0.0")
RUNN_TIME_ENTRIES_PATH = os.getenv("RUNN_TIME_ENTRIES_PATH", "timesheets")  # cambia a "timesheets" si tu cuenta lo usa
BQ_PROJECT = os.getenv("BQ_PROJECT")
BQ_DATASET = os.getenv("BQ_DATASET", "people_analytics")
BQ_TABLE = os.getenv("BQ_TABLE", "runn_time_entries")
TOKEN = os.getenv("RUNN_API_TOKEN")
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "30"))

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json",
}

def _date_range(window_days: int):
    end = dt.date.today()
    start = end - dt.timedelta(days=window_days)
    return start.isoformat(), end.isoformat()

def _fetch_page(url, params):
    r = requests.get(url, headers=HEADERS, params=params, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()

def _iter_entries(window_days: int):
    start, end = _date_range(window_days)
    base_url = f"{RUNN_API}/{RUNN_API_VERSION}/{RUNN_TIME_ENTRIES_PATH}"
    page = 1
    while True:
        payload = _fetch_page(base_url, {"from": start, "to": end, "page": page, "per_page": 200})
        data = payload if isinstance(payload, list) else payload.get("data", [])
        if not data:
            break
        for row in data:
            yield row
        page += 1
        # Por si la API devuelve total_pages
        total_pages = payload.get("total_pages") if isinstance(payload, dict) else None
        if total_pages and page > total_pages:
            break

def _normalize(rec: dict):
    # Ajusta campos según tu cuenta; deja llaves “planas” que usarás en Looker
    return {
        "entry_id": rec.get("id"),
        "person_id": rec.get("personId") or rec.get("person_id"),
        "person_name": rec.get("personName") or rec.get("person_name"),
        "project_id": rec.get("projectId") or rec.get("project_id"),
        "project_name": rec.get("projectName") or rec.get("project_name"),
        "client_name": rec.get("clientName") or rec.get("client_name"),
        "role": rec.get("role"),
        "date": rec.get("date"),  # YYYY-MM-DD
        "hours": float(rec.get("hours") or 0),
        "billable": bool(rec.get("billable")) if rec.get("billable") is not None else None,
        "tags": ",".join(rec.get("tags", [])) if isinstance(rec.get("tags"), list) else rec.get("tags"),
        "created_at": rec.get("createdAt") or rec.get("created_at"),
        "updated_at": rec.get("updatedAt") or rec.get("updated_at"),
        "_ingested_at": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }

def _ensure_table(client: bigquery.Client):
    schema = [
        bigquery.SchemaField("entry_id", "STRING"),
        bigquery.SchemaField("person_id", "STRING"),
        bigquery.SchemaField("person_name", "STRING"),
        bigquery.SchemaField("project_id", "STRING"),
        bigquery.SchemaField("project_name", "STRING"),
        bigquery.SchemaField("client_name", "STRING"),
        bigquery.SchemaField("role", "STRING"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("hours", "FLOAT"),
        bigquery.SchemaField("billable", "BOOL"),
        bigquery.SchemaField("tags", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
        bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
    ]
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    try:
        client.get_table(table_id)
    except Exception:
        client.create_dataset(bigquery.Dataset(f"{BQ_PROJECT}.{BQ_DATASET}"), exists_ok=True)
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table, exists_ok=True)
    return table_id

def _upsert(client: bigquery.Client, table_id: str, rows: list[dict]):
    # Carga por JSON; si hay duplicados, luego deduplicamos con SQL (MERGE si lo prefieres)
    job = client.load_table_from_json(rows, table_id, job_config=bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    ))
    job.result()

def export_handler(window_days: int = 90, **_):
    if not all([BQ_PROJECT, BQ_DATASET, TOKEN]):
        raise RuntimeError("Faltan BQ_PROJECT/BQ_DATASET/RUNN_API_TOKEN")
    client = bigquery.Client(project=BQ_PROJECT)
    table_id = _ensure_table(client)

    batch, inserted, pages = [], 0, 0
    for raw in _iter_entries(window_days):
        batch.append(_normalize(raw))
        if len(batch) >= 2000:
            _upsert(client, table_id, batch)
            inserted += len(batch)
            batch = []
            pages += 1
    if batch:
        _upsert(client, table_id, batch)
        inserted += len(batch)

    return {"ok": True, "inserted": inserted, "table": table_id, "window_days": window_days}
