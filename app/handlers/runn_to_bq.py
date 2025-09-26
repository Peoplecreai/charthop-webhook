# handlers/runn_to_bq.py
import os
import math
import time
import datetime as dt
from typing import Dict, Any, List, Optional

import requests
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# ---------- Config ----------
RUNN_API = os.environ.get("RUNN_API", "https://api.runn.io").rstrip("/")
RUNN_API_VERSION = os.environ.get("RUNN_API_VERSION", "1.0.0")
RUNN_API_TOKEN = os.environ.get("RUNN_API_TOKEN")  # viene de Secret Manager
HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "30"))

BQ_PROJECT = os.environ.get("BQ_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")

# tablas
FINAL_TABLE = "runn_actuals"
STAGING_TABLE = "_stg_runn_actuals"

# ---------- Esquema BigQuery ----------
BQ_SCHEMA = [
    bigquery.SchemaField("id", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("billableMinutes", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("nonbillableMinutes", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("billableNote", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("nonbillableNote", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("phaseId", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("personId", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("projectId", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("roleId", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("workstreamId", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("createdAt", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("updatedAt", "TIMESTAMP", mode="REQUIRED"),
]

# ---------- Utilidad Runn ----------
def _runn_headers() -> Dict[str, str]:
    if not RUNN_API_TOKEN:
        raise RuntimeError("RUNN_API_TOKEN no está definido")
    return {
        "accept": "application/json",
        "accept-version": RUNN_API_VERSION,
        "authorization": f"Bearer {RUNN_API_TOKEN}",
    }

def fetch_runn_actuals(min_date: str) -> List[Dict[str, Any]]:
    """
    Descarga /actuals con paginación por cursor desde Runn.
    """
    url = f"{RUNN_API}/actuals/"
    headers = _runn_headers()

    all_rows: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    session = requests.Session()

    while True:
        params = {"minDate": min_date}
        if cursor:
            params["cursor"] = cursor

        resp = session.get(url, headers=headers, params=params, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()

        values = payload.get("values", [])
        all_rows.extend(values)

        cursor = payload.get("nextCursor")
        if not cursor:
            break

        # pequeña pausa por cortesía
        time.sleep(0.05)

    return all_rows

# ---------- Utilidad BigQuery ----------
def _table_ref(project: str, dataset: str, table: str) -> str:
    return f"{project}.{dataset}.{table}"

def ensure_table(client: bigquery.Client, table_id: str, schema: List[bigquery.SchemaField]) -> None:
    try:
        client.get_table(table_id)
    except NotFound:
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)

def truncate_table(client: bigquery.Client, table_id: str) -> None:
    client.query(f"TRUNCATE TABLE `{table_id}`", location=client.location).result()

def load_json_to_table(
    client: bigquery.Client,
    rows: List[Dict[str, Any]],
    table_id: str,
    schema: List[bigquery.SchemaField],
) -> bigquery.job.LoadJob:
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    return client.load_table_from_json(rows, destination=table_id, job_config=job_config)

def merge_staging_into_final(client: bigquery.Client, project: str, dataset: str) -> None:
    final_id = _table_ref(project, dataset, FINAL_TABLE)
    staging_id = _table_ref(project, dataset, STAGING_TABLE)

    merge_sql = f"""
    MERGE `{final_id}` T
    USING `{staging_id}` S
    ON T.id = S.id
    WHEN MATCHED THEN UPDATE SET
      date = S.date,
      billableMinutes = S.billableMinutes,
      nonbillableMinutes = S.nonbillableMinutes,
      billableNote = S.billableNote,
      nonbillableNote = S.nonbillableNote,
      phaseId = S.phaseId,
      personId = S.personId,
      projectId = S.projectId,
      roleId = S.roleId,
      workstreamId = S.workstreamId,
      createdAt = S.createdAt,
      updatedAt = S.updatedAt
    WHEN NOT MATCHED THEN INSERT (
      id, date, billableMinutes, nonbillableMinutes, billableNote, nonbillableNote,
      phaseId, personId, projectId, roleId, workstreamId, createdAt, updatedAt
    ) VALUES (
      S.id, S.date, S.billableMinutes, S.nonbillableMinutes, S.billableNote, S.nonbillableNote,
      S.phaseId, S.personId, S.projectId, S.roleId, S.workstreamId, S.createdAt, S.updatedAt
    );
    """
    job = client.query(merge_sql, location=client.location)
    job.result()

# ---------- Handler público ----------
def export_handler(window_days: int = 120, **_) -> Dict[str, Any]:
    """
    1) Pull de Runn /actuals (desde minDate = hoy - window_days)
    2) Carga a _stg_runn_actuals
    3) MERGE a runn_actuals
    Devuelve resumen.
    """
    # Validación de config mínima
    for k in ("BQ_PROJECT", "BQ_DATASET"):
        if not globals().get(k):
            raise RuntimeError(f"{k} no está definido")

    today = dt.date.today()
    min_date = (today - dt.timedelta(days=int(window_days))).strftime("%Y-%m-%d")

    # 1) Fetch
    rows = fetch_runn_actuals(min_date=min_date)

    # 2) Staging -> BigQuery
    client = bigquery.Client(project=BQ_PROJECT, location=BQ_LOCATION)

    staging_id = _table_ref(BQ_PROJECT, BQ_DATASET, STAGING_TABLE)
    final_id = _table_ref(BQ_PROJECT, BQ_DATASET, FINAL_TABLE)

    # Asegura tablas
    ensure_table(client, final_id, BQ_SCHEMA)   # final
    ensure_table(client, staging_id, BQ_SCHEMA) # staging

    # Limpia staging y carga
    truncate_table(client, staging_id)

    # Normaliza tipos (por seguridad; requests ya trae ints/strings)
    def normalize(x: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": int(x["id"]),
            "date": x["date"],  # YYYY-MM-DD
            "billableMinutes": int(x.get("billableMinutes", 0) or 0),
            "nonbillableMinutes": int(x.get("nonbillableMinutes", 0) or 0),
            "billableNote": x.get("billableNote"),
            "nonbillableNote": x.get("nonbillableNote"),
            "phaseId": (int(x["phaseId"]) if x.get("phaseId") is not None else None),
            "personId": int(x["personId"]),
            "projectId": int(x["projectId"]),
            "roleId": int(x["roleId"]),
            "workstreamId": (int(x["workstreamId"]) if x.get("workstreamId") is not None else None),
            "createdAt": x["createdAt"],
            "updatedAt": x["updatedAt"],
        }

    norm_rows = [normalize(r) for r in rows]

    load_job = load_json_to_table(client, norm_rows, staging_id, BQ_SCHEMA)
    load_job.result()

    # 3) MERGE -> final
    merge_staging_into_final(client, BQ_PROJECT, BQ_DATASET)

    return {
        "ok": True,
        "entity": "runn_actuals",
        "window_days": int(window_days),
        "minDate": min_date,
        "staging_loaded_rows": len(norm_rows),
        "bq_project": BQ_PROJECT,
        "bq_dataset": BQ_DATASET,
        "bq_location": BQ_LOCATION,
        "details": "Staging cargado y MERGE completado",
    }
