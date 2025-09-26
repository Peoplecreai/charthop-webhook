"""Handlers for exporting RUNN actuals to BigQuery."""

import datetime
import os
from typing import Any

import requests
from google.cloud import bigquery


def export_handler(window_days: int = 90, **_: Any) -> dict[str, Any]:
    """Fetch RUNN actuals for the given window and load them into BigQuery."""
    base = os.environ.get("RUNN_API", "https://api.runn.io").rstrip("/")
    path = os.environ.get("RUNN_TIME_ENTRIES_PATH", "actuals").lstrip("/")
    url = f"{base}/{path}"

    token = os.environ["RUNN_API_TOKEN"]
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Version": os.environ.get("RUNN_API_VERSION", "1.0.0"),
    }

    today = datetime.date.today()
    min_date = (today - datetime.timedelta(days=window_days)).isoformat()
    max_date = today.isoformat()

    rows: list[dict[str, Any]] = []
    cursor: str | None = None
    session = requests.Session()
    timeout = int(os.environ.get("HTTP_TIMEOUT", "30"))

    while True:
        params: dict[str, Any] = {"minDate": min_date, "maxDate": max_date, "limit": 200}
        if cursor:
            params["cursor"] = cursor

        response = session.get(url, headers=headers, params=params, timeout=timeout)
        response.raise_for_status()
        payload = response.json()

        items = payload.get("items") if isinstance(payload, dict) else payload
        if not items:
            break

        for actual in items:
            rows.append(
                {
                    "id": actual.get("id"),
                    "date": actual.get("date"),
                    "hours": actual.get("hours"),
                    "projectId": actual.get("projectId"),
                    "personId": actual.get("personId"),
                    "roleId": actual.get("roleId"),
                    "phaseId": actual.get("phaseId"),
                    "note": actual.get("note"),
                    "createdAt": actual.get("createdAt"),
                    "updatedAt": actual.get("updatedAt"),
                }
            )

        cursor = payload.get("nextCursor") if isinstance(payload, dict) else None
        if not cursor:
            break

    if not rows:
        return {"ok": True, "result": "sin actuals en rango", "window_days": window_days}

    bq = bigquery.Client(project=os.environ["BQ_PROJECT"])
    dataset = os.environ.get("BQ_DATASET", "people_analytics")
    table_id = f"{bq.project}.{dataset}.runn_actuals"

    dataset_id = f"{bq.project}.{dataset}"
    try:
        bq.get_dataset(dataset_id)
    except Exception:
        bq.create_dataset(bigquery.Dataset(dataset_id), exists_ok=True)

    job = bq.load_table_from_json(
        rows,
        table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
        ),
    )
    job.result()

    return {"ok": True, "rows": len(rows), "table": table_id, "window_days": window_days}
