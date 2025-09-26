# handlers/runn_to_bq.py
from __future__ import annotations
import os, requests
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Iterator, List
from google.cloud import bigquery

RUNN_API = os.getenv("RUNN_API", "https://api.runn.io")
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "30"))
BQ_PROJECT = os.environ["BQ_PROJECT"]
BQ_DATASET = os.getenv("BQ_DATASET", "people_analytics")

def _headers() -> Dict[str, str]:
    token = os.environ["RUNN_API_TOKEN"]
    version = os.getenv("RUNN_API_VERSION", "1.0.0")
    return {
        "Authorization": f"Bearer {token}",
        "Accept-Version": version,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

def _get(path: str, params: Dict[str, Any] | None = None):
    r = requests.get(f"{RUNN_API.rstrip('/')}/{path.lstrip('/')}",
                     headers=_headers(), params=params or {}, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()

def _paginate(path: str, base_params: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
    page = 1
    while True:
        params = dict(base_params or {})
        params.setdefault("per_page", 100)
        params["page"] = page
        data = _get(path, params)
        items = data.get("items") if isinstance(data, dict) else data
        if not items:
            break
        for it in items:
            yield it
        if len(items) < params["per_page"]:
            break
        page += 1

def _time_entries(since_iso: str, until_iso: str) -> Iterator[Dict[str, Any]]:
    path = os.getenv("RUNN_TIME_ENTRIES_PATH", "time-entries")  # o "timesheets"
    yield from _paginate(path, {"since": since_iso, "until": until_iso})

def export_handler(*, window_days: int = 120) -> Dict[str, Any]:
    tz = timezone.utc
    until_dt = datetime.now(tz=tz).replace(microsecond=0)
    since_dt = until_dt - timedelta(days=window_days)
    since_iso, until_iso = since_dt.isoformat(), until_dt.isoformat()

    client = bigquery.Client(project=BQ_PROJECT)
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.runn_time_entries"

    schema = [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("person_id", "STRING"),
        bigquery.SchemaField("project_id", "STRING"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("hours", "FLOAT"),
        bigquery.SchemaField("billable", "BOOL"),
        bigquery.SchemaField("role", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
        bigquery.SchemaField("raw", "JSON"),
    ]
    try:
        client.get_table(table_id)
    except Exception:
        client.create_table(bigquery.Table(table_id, schema=schema))

    rows: List[Dict[str, Any]] = []
    for te in _time_entries(since_iso, until_iso):
        rows.append({
            "id": str(te.get("id")),
            "person_id": str(te.get("personId") or te.get("person_id") or ""),
            "project_id": str(te.get("projectId") or te.get("project_id") or ""),
            "date": te.get("date"),
            "hours": float(te.get("hours") or 0.0),
            "billable": bool(te.get("billable")),
            "role": te.get("role") or None,
            "created_at": te.get("createdAt") or te.get("created_at"),
            "updated_at": te.get("updatedAt") or te.get("updated_at"),
            "raw": te,
        })

    inserted = 0
    if rows:
        batch = 500
        for i in range(0, len(rows), batch):
            part = rows[i:i+batch]
            errors = client.insert_rows_json(table_id, part)
            if errors:
                return {"ok": False, "since": since_iso, "until": until_iso, "errors": errors}
            inserted += len(part)

    return {"ok": True, "inserted": inserted, "since": since_iso, "until": until_iso}
