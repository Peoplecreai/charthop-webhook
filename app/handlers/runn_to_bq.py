# handlers/runn_to_bq.py
import os, datetime, requests
from google.cloud import bigquery

RUNN_API = os.environ.get("RUNN_API", "https://api.runn.io")
RUNN_API_VERSION = os.environ.get("RUNN_API_VERSION", "1.0.0")
BQ_PROJECT = os.environ["BQ_PROJECT"]
BQ_DATASET = os.environ["BQ_DATASET"]
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")
HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "30"))

TABLE_ID = f"{BQ_PROJECT}.{BQ_DATASET}.runn_actuals"

def _headers(token: str):
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Accept-Version": RUNN_API_VERSION,
    }

def _daterange(window_days: int):
    to = datetime.date.today()
    frm = to - datetime.timedelta(days=window_days)
    return frm.isoformat(), to.isoformat()

def _fetch_actuals(token: str, min_date: str, max_date: str):
    url = f"{RUNN_API}/actuals"
    cursor = None
    session = requests.Session()
    out = []
    while True:
        params = {
            "minDate": min_date,
            "maxDate": max_date,
            "limit": 200,
        }
        if cursor:
            params["cursor"] = cursor
        r = session.get(url, headers=_headers(token), params=params, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        vals = data.get("values", [])
        out.extend(vals)
        cursor = data.get("nextCursor")
        if not cursor:
            break
    return out

def _ensure_table():
    client = bigquery.Client(project=BQ_PROJECT, location=BQ_LOCATION)
    schema = [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("hours", "FLOAT"),
        bigquery.SchemaField("personId", "STRING"),
        bigquery.SchemaField("projectId", "STRING"),
        bigquery.SchemaField("roleId", "STRING"),
        bigquery.SchemaField("createdAt", "TIMESTAMP"),
        bigquery.SchemaField("updatedAt", "TIMESTAMP"),
        # agrega campos según lo que devuelva tu cuenta (safe-by-default)
        bigquery.SchemaField("raw", "JSON"),
    ]
    table = bigquery.Table(TABLE_ID, schema=schema)
    table = client.create_table(table, exists_ok=True)

def _rows(values):
    rows = []
    for v in values:
        rows.append({
            "id": str(v.get("id")),
            "date": v.get("date"),
            "hours": v.get("hours"),
            "personId": str(v.get("personId")) if v.get("personId") is not None else None,
            "projectId": str(v.get("projectId")) if v.get("projectId") is not None else None,
            "roleId": str(v.get("roleId")) if v.get("roleId") is not None else None,
            "createdAt": v.get("createdAt"),
            "updatedAt": v.get("updatedAt"),
            "raw": v,
        })
    return rows

def export_handler(window_days: int = 90, **_):
    token = os.environ.get("RUNN_API_TOKEN")
    if not token:
        return {"ok": False, "reason": "Falta RUNN_API_TOKEN"}
    min_date, max_date = _daterange(window_days)
    try:
        values = _fetch_actuals(token, min_date, max_date)
    except requests.HTTPError as e:
        return {"ok": False, "reason": f"HTTP {e.response.status_code}: {e.response.text}"}
    except Exception as e:
        return {"ok": False, "reason": str(e)}

    _ensure_table()
    client = bigquery.Client(project=BQ_PROJECT, location=BQ_LOCATION)
    errors = client.insert_rows_json(TABLE_ID, _rows(values))
    if errors:
        return {"ok": False, "reason": "Errores al insertar en BQ", "details": errors}
    return {"ok": True, "count": len(values), "minDate": min_date, "maxDate": max_date}
