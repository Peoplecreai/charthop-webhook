from __future__ import annotations
import os, argparse, datetime as dt, time, json, requests
from google.cloud import bigquery

API = "https://api.runn.io"
HDRS = {
    "Authorization": f"Bearer {os.environ['RUNN_API_TOKEN']}",
    "Accept-Version": "1.0.0",
    "Accept": "application/json",
}
PROJ = os.environ["BQ_PROJECT"]
DS   = os.environ["BQ_DATASET"]

COLLS = {
    "runn_people": "/people/",
    "runn_projects": "/projects/",
    "runn_clients": "/clients/",
    "runn_roles": "/roles/",
    "runn_teams": "/teams/",
    "runn_skills": "/skills/",
    "runn_people_tags": "/people-tags/",
    "runn_project_tags": "/project-tags/",
    "runn_rate_cards": "/rate-cards/",
    "runn_workstreams": "/workstreams/",
    "runn_assignments": "/assignments/",
    "runn_actuals": "/actuals/",
    "runn_timeoffs_leave": "/time-offs/leave/",
    "runn_timeoffs_rostered": "/time-offs/rostered/",
    "runn_timeoffs_holidays": "/time-offs/holidays/",
}

def state_table() -> str:
    return f"{PROJ}.{DS}.__runn_sync_state"

def ensure_state_table(bq: bigquery.Client):
    bq.query(f"""
    CREATE TABLE IF NOT EXISTS `{state_table()}`(
      table_name STRING NOT NULL,
      last_success TIMESTAMP,
      PRIMARY KEY(table_name) NOT ENFORCED
    )""").result()

def get_last_success(bq: bigquery.Client, name: str):
    q = bq.query(
        f"SELECT last_success FROM `{state_table()}` WHERE table_name=@t",
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("t", "STRING", name)]
        ),
    ).result()
    for r in q:
        return r[0]
    return None

def set_last_success(bq: bigquery.Client, name: str, ts: dt.datetime):
    bq.query(
      f"""
      MERGE `{state_table()}` T
      USING (SELECT @t AS table_name, @ts AS last_success) S
      ON T.table_name=S.table_name
      WHEN MATCHED THEN UPDATE SET last_success=S.last_success
      WHEN NOT MATCHED THEN INSERT(table_name,last_success) VALUES(S.table_name,S.last_success)
      """,
      job_config=bigquery.QueryJobConfig(
        query_parameters=[
          bigquery.ScalarQueryParameter("t","STRING",name),
          bigquery.ScalarQueryParameter("ts","TIMESTAMP",ts.isoformat())
        ]
      )
    ).result()

def fetch_all(path: str, since_iso: str|None, limit=200):
    s = requests.Session(); s.headers.update(HDRS)
    out, cursor = [], None
    while True:
        params = {"limit": limit}
        # ahorro de tráfico en colecciones grandes (si el endpoint lo soporta)
        if since_iso and path.rstrip("/").split("/")[-1] in {"actuals","assignments"}:
            params["modifiedAfter"] = since_iso
        if cursor:
            params["cursor"] = cursor
        r = s.get(API + path, params=params, timeout=60)
        if r.status_code == 429:
            time.sleep(int(r.headers.get("Retry-After", "5") or "5"))
            continue
        r.raise_for_status()
        payload = r.json()
        values = payload.get("values", payload if isinstance(payload, list) else [])
        if isinstance(values, dict):
            values = [values]
        out.extend(values)
        cursor = payload.get("nextCursor")
        if not cursor:
            break
    return out

def load_merge(table_base: str, rows: list[dict], bq: bigquery.Client):
    if not rows:
        return 0

    stg = f"{PROJ}.{DS}._stg__{table_base}"
    tgt = f"{PROJ}.{DS}.{table_base}"

    # 1) Carga a staging con autodetección de esquema
    job = bq.load_table_from_json(
        rows,
        stg,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True
        )
    )
    job.result()

    stg_tbl = bq.get_table(stg)
    stg_schema = stg_tbl.schema

    # 2) Asegura existencia de la tabla target (con esquema inicial igual al de staging)
    try:
        bq.get_table(tgt)
    except Exception:
        bq.create_table(bigquery.Table(tgt, schema=stg_schema))

    # 3) MERGE por id (robusto a tipos distintos haciendo CAST a STRING en ambos lados)
    has_id = any(c.name == "id" for c in stg_schema)
    if has_id:
        cols = [c.name for c in stg_schema]
        non_id_cols = [c for c in cols if c != "id"]

        # SET columna-a-columna (sin tocar id)
        set_clause  = ", ".join([f"T.{c}=S.{c}" for c in non_id_cols])

        # INSERT con columnas explícitas; casteo de id a STRING por seguridad
        insert_cols = ", ".join(["id"] + non_id_cols)
        insert_vals = ", ".join(["CAST(S.id AS STRING)"] + [f"S.{c}" for c in non_id_cols])

        sql = f"""
        MERGE `{tgt}` AS T
        USING `{stg}` AS S
        ON CAST(T.id AS STRING) = CAST(S.id AS STRING)
        WHEN MATCHED THEN
          UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN
          INSERT ({insert_cols}) VALUES ({insert_vals})
        """
    else:
        # si no hay id, reemplazo completo
        sql = f"CREATE OR REPLACE TABLE `{tgt}` AS SELECT * FROM `{stg}`"

    bq.query(sql).result()
    return bq.get_table(tgt).num_rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["full","delta"], default="delta")
    ap.add_argument("--delta-days", type=int, default=90)
    ap.add_argument("--only", help="coma-separado: runn_people,runn_projects,…")
    args = ap.parse_args()

    bq = bigquery.Client(project=PROJ)
    ensure_state_table(bq)

    now = dt.datetime.now(dt.timezone.utc)

    targets = COLLS if not args.only else {
        k: COLLS[k] for k in args.only.split(",") if k in COLLS
    }

    summary = {}
    for tbl, path in targets.items():
        since_iso = None
        if args.mode == "delta":
            last = get_last_success(bq, tbl)
            if last:
                since_iso = last.strftime("%Y-%m-%dT%H:%M:%SZ")
            elif tbl in {"runn_actuals","runn_assignments"}:
                # primera corrida delta de colecciones grandes: ventana de seguridad
                since_iso = (now - dt.timedelta(days=args.delta_days)).strftime("%Y-%m-%dT%H:%M:%SZ")

        rows = fetch_all(path, since_iso)
        n = load_merge(tbl, rows, bq)
        summary[tbl] = int(n)

        # Marca watermark de éxito por tabla (último now)
        set_last_success(bq, tbl, now)

    print(json.dumps({"ok": True, "loaded": summary}, ensure_ascii=False))

if __name__ == "__main__":
    main()
