from __future__ import annotations
import os, argparse, datetime as dt, time, json, requests
from typing import Dict, List, Tuple, Optional, Union
from google.cloud import bigquery

# -----------------------
# Config HTTP / Entorno
# -----------------------
API = "https://api.runn.io"
HDRS = {
    "Authorization": f"Bearer {os.environ['RUNN_API_TOKEN']}",
    "Accept-Version": "1.0.0",
    "Accept": "application/json",
}
PROJ = os.environ["BQ_PROJECT"]
DS   = os.environ["BQ_DATASET"]

# -----------------------------------------------------------------------------
# Catálogo de colecciones.
# Valor puede ser:
#   - str  -> solo path
#   - (path, {params}) -> path + parámetros fijos de query
# -----------------------------------------------------------------------------
COLLS: Dict[str, Union[str, Tuple[str, Dict[str, str]]]] = {
    # Base (ya existentes en tu repo)
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

    # --- Nuevas que pediste ---
    # Grupos de feriados
    "runn_holiday_groups": "/holiday-groups/",
    # Placeholders (admite modifiedAfter)
    "runn_placeholders": ("/placeholders/", {}),
    # Contracts (admite modifiedAfter; sortBy requerido/seguro)
    "runn_contracts": ("/contracts/", {"sortBy": "id"}),

    # Custom Fields por TIPO + model (según la doc v1.0)
    # Ejemplo con checkbox; puedes añadir text/enum/number/date replicando el patrón.
    "runn_custom_fields_checkbox_person":  ("/custom-fields/checkbox/", {"model": "PERSON"}),
    "runn_custom_fields_checkbox_project": ("/custom-fields/checkbox/", {"model": "PROJECT"}),
}

# -----------------------
# Estado de sync en BQ
# -----------------------
def state_table() -> str:
    return f"{PROJ}.{DS}.__runn_sync_state"

def ensure_state_table(bq: bigquery.Client):
    bq.query(f"""
    CREATE TABLE IF NOT EXISTS `{state_table()}`(
      table_name STRING NOT NULL,
      last_success TIMESTAMP,
      PRIMARY KEY(table_name) NOT ENFORCED
    )""").result()

def get_last_success(bq: bigquery.Client, name: str) -> Optional[dt.datetime]:
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

# -----------------------
# Descarga paginada
# -----------------------
def _supports_modified_after(path: str) -> bool:
    tail = path.rstrip("/").split("/")[-1]
    # según docs: actuals, assignments, contracts, placeholders aceptan modifiedAfter
    return tail in {"actuals", "assignments", "contracts", "placeholders"}

def fetch_all(path: str, since_iso: Optional[str], limit=200, extra_params: Optional[Dict[str,str]]=None) -> List[Dict]:
    s = requests.Session(); s.headers.update(HDRS)
    out: List[Dict] = []
    cursor: Optional[str] = None

    while True:
        params: Dict[str, str] = {"limit": str(limit)}
        if extra_params:
            params.update(extra_params)
        if since_iso and _supports_modified_after(path):
            params["modifiedAfter"] = since_iso
        if cursor:
            params["cursor"] = cursor

        r = s.get(API + path, params=params, timeout=60)
        if r.status_code == 429:
            time.sleep(int(r.headers.get("Retry-After", "5") or "5"))
            continue
        if r.status_code == 404:
            # endpoint no disponible en el tenant/plan; no rompemos el job
            if os.environ.get("RUNN_DEBUG"):
                print(f"[WARN] 404 Not Found: {API+path} params={params}  (ignorado)")
            return []
        r.raise_for_status()

        payload = r.json()
        values = payload.get("values", payload if isinstance(payload, list) else [])
        if isinstance(values, dict):
            values = [values]
        out.extend(values)

        cursor = payload.get("nextCursor")
        if not cursor:
            break

    if os.environ.get("RUNN_DEBUG"):
        print(f"[INFO] fetched {len(out)} rows from {path} (params={extra_params or {}})")
    return out

# -----------------------
# Carga y MERGE a BQ
# -----------------------
def load_merge(table_base: str, rows: List[Dict], bq: bigquery.Client) -> int:
    if not rows:
        return 0

    stg = f"{PROJ}.{DS}._stg__{table_base}"
    tgt = f"{PROJ}.{DS}.{table_base}"

    # 1) Carga staging con autodetección
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

    # 2) Asegura target
    try:
        bq.get_table(tgt)
    except Exception:
        bq.create_table(bigquery.Table(tgt, schema=stg_schema))

    # 3) MERGE por id (cast a STRING para evitar choques INT64/STRING)
    has_id = any(c.name == "id" for c in stg_schema)
    if has_id:
        cols = [c.name for c in stg_schema]
        non_id_cols = [c for c in cols if c != "id"]

        set_clause  = ", ".join([f"T.{c}=S.{c}" for c in non_id_cols])
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
        sql = f"CREATE OR REPLACE TABLE `{tgt}` AS SELECT * FROM `{stg}`"

    bq.query(sql).result()
    return bq.get_table(tgt).num_rows

# -----------------------
# CLI
# -----------------------
def parse_only(raw: Optional[List[str]]) -> Optional[List[str]]:
    """Acepta --only repetido o lista separada por comas."""
    if not raw:
        return None
    out: List[str] = []
    for item in raw:
        out.extend([p.strip() for p in item.split(",") if p.strip()])
    # dedup manteniendo orden
    seen = set(); ordered = []
    for k in out:
        if k not in seen:
            seen.add(k); ordered.append(k)
    return ordered

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["full","delta"], default="delta")
    ap.add_argument("--delta-days", type=int, default=90)
    ap.add_argument("--only", action="append", help="repetible o coma-separado: runn_people,runn_projects,…")
    args = ap.parse_args()

    only_list = parse_only(args.only)

    bq = bigquery.Client(project=PROJ)
    ensure_state_table(bq)

    now = dt.datetime.now(dt.timezone.utc)

    targets: Dict[str, Union[str, Tuple[str, Dict[str,str]]]]
    if not only_list:
        targets = COLLS
    else:
        targets = {k: COLLS[k] for k in only_list if k in COLLS}

    summary: Dict[str, int] = {}
    for tbl, spec in targets.items():
        if isinstance(spec, tuple):
            path, extra = spec
        else:
            path, extra = spec, None

        since_iso: Optional[str] = None
        if args.mode == "delta":
            last = get_last_success(bq, tbl)
            if last:
                since_iso = last.strftime("%Y-%m-%dT%H:%M:%SZ")
            elif path.rstrip("/").split("/")[-1] in {"actuals","assignments","contracts","placeholders"}:
                since_iso = (now - dt.timedelta(days=args.delta_days)).strftime("%Y-%m-%dT%H:%M:%SZ")

        rows = fetch_all(path, since_iso, extra_params=extra)
        n = load_merge(tbl, rows, bq)
        summary[tbl] = int(n)
        set_last_success(bq, tbl, now)

    print(json.dumps({"ok": True, "loaded": summary}, ensure_ascii=False))

if __name__ == "__main__":
    main()
