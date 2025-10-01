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

RUNN_HOLIDAY_GROUP_ID = os.environ.get("RUNN_HOLIDAY_GROUP_ID")  # opcional

# -----------------------------------------------------------------------------
# Catálogo de colecciones.
# Valor puede ser:
#   - str  -> solo path
#   - (path, {params}) -> path + parámetros fijos de query
# -----------------------------------------------------------------------------
COLLS: Dict[str, Union[str, Tuple[str, Dict[str, str]]]] = {
    # Base
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

    # Nuevas
    "runn_holiday_groups": "/holiday-groups/",
    "runn_placeholders": ("/placeholders/", {}),
    "runn_contracts": ("/contracts/", {"sortBy": "id"}),

    # Custom Fields (ejemplos)
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
# Helpers
# -----------------------
def _supports_modified_after(path: str) -> bool:
    tail = path.rstrip("/").split("/")[-1]
    # según docs: actuals, assignments, contracts, placeholders aceptan modifiedAfter
    return tail in {"actuals", "assignments", "contracts", "placeholders"}

def _accepts_date_window(path: str) -> bool:
    # endpoints que aceptan dateFrom/dateTo/personId
    tail = path.rstrip("/").split("/")[-1]
    return tail in {"actuals", "assignments"}

def fetch_all(path: str,
              since_iso: Optional[str],
              limit=200,
              extra_params: Optional[Dict[str,str]]=None) -> List[Dict]:
    s = requests.Session(); s.headers.update(HDRS)
    out: List[Dict] = []
    cursor: Optional[str] = None

    while True:
        params: Dict[str, str] = {"limit": str(limit)}
        if extra_params:
            params.update({k: v for k, v in extra_params.items() if v is not None and v != ""})
        if since_iso and _supports_modified_after(path):
            params["modifiedAfter"] = since_iso
        if cursor:
            params["cursor"] = cursor

        r = s.get(API + path, params=params, timeout=60)
        if r.status_code == 429:
            time.sleep(int(r.headers.get("Retry-After", "5") or "5"))
            continue
        if r.status_code == 404:
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
# BQ: purga de ventana (para backfill)
# -----------------------
def purge_scope(bq: bigquery.Client,
                table_base: str,
                person: Optional[str],
                dfrom: Optional[str],
                dto: Optional[str]) -> None:
    if not (dfrom and dto):
        return
    if table_base not in {"runn_actuals", "runn_assignments"}:
        return

    if person:
        q = f"""
        DELETE FROM `{PROJ}.{DS}.{table_base}`
        WHERE CAST(personId AS STRING)=@p
          AND DATE(date) BETWEEN @d1 AND @d2
        """
        params = [
            bigquery.ScalarQueryParameter("p","STRING", person),
            bigquery.ScalarQueryParameter("d1","DATE", dfrom),
            bigquery.ScalarQueryParameter("d2","DATE", dto),
        ]
    else:
        q = f"""
        DELETE FROM `{PROJ}.{DS}.{table_base}`
        WHERE DATE(date) BETWEEN @d1 AND @d2
        """
        params = [
            bigquery.ScalarQueryParameter("d1","DATE", dfrom),
            bigquery.ScalarQueryParameter("d2","DATE", dto),
        ]
    bq.query(q, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()

# -----------------------
# Carga y MERGE a BQ
# -----------------------
def _create_empty_timeoff_table_if_needed(table_base: str, bq: bigquery.Client) -> None:
    if table_base not in {"runn_timeoffs_leave", "runn_timeoffs_rostered"}:
        return
    tgt = f"{PROJ}.{DS}.{table_base}"
    try:
        bq.get_table(tgt)
        return
    except Exception:
        pass

    schema = [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("personId", "STRING"),
        bigquery.SchemaField("startDate", "DATE"),
        bigquery.SchemaField("endDate", "DATE"),
        bigquery.SchemaField("note", "STRING"),
        bigquery.SchemaField("createdAt", "TIMESTAMP"),
        bigquery.SchemaField("updatedAt", "TIMESTAMP"),
        bigquery.SchemaField("minutesPerDay", "INT64"),
    ]
    bq.create_table(bigquery.Table(tgt, schema=schema))

def load_merge(table_base: str, rows: List[Dict], bq: bigquery.Client) -> int:
    if not rows:
        _create_empty_timeoff_table_if_needed(table_base, bq)
        return 0

    stg = f"{PROJ}.{DS}._stg__{table_base}"
    tgt = f"{PROJ}.{DS}.{table_base}"

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

    try:
        bq.get_table(tgt)
    except Exception:
        bq.create_table(bigquery.Table(tgt, schema=stg_schema))

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
# CLI helpers
# -----------------------
def parse_only(raw: Optional[List[str]]) -> Optional[List[str]]:
    if not raw:
        return None
    out: List[str] = []
    for item in raw:
        out.extend([p.strip() for p in item.split(",") if p.strip()])
    seen = set(); ordered = []
    for k in out:
        if k not in seen:
            seen.add(k); ordered.append(k)
    return ordered

# -----------------------
# MAIN
# -----------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["full","delta"], default="delta")
    ap.add_argument("--delta-days", type=int, default=90)
    ap.add_argument("--overlap-days", type=int, default=7, help="relee últimos N días en delta")
    ap.add_argument("--only", action="append", help="repetible o coma-separado: runn_people,runn_projects,…")

    # backfill dirigido por rango/persona (para actuals/assignments)
    ap.add_argument("--range-from", dest="range_from", help="YYYY-MM-DD")
    ap.add_argument("--range-to",   dest="range_to",   help="YYYY-MM-DD")
    ap.add_argument("--person-id",  dest="person_id",  help="filtrar por persona en backfill")

    args = ap.parse_args()
    only_list = parse_only(args.only)

    bq = bigquery.Client(project=PROJ)
    ensure_state_table(bq)

    now = dt.datetime.now(dt.timezone.utc)

    # Inyecta param de filtro para holidays si viene RUNN_HOLIDAY_GROUP_ID
    collections: Dict[str, Union[str, Tuple[str, Dict[str, str]]]] = dict(COLLS)
    if RUNN_HOLIDAY_GROUP_ID:
        collections["runn_timeoffs_holidays"] = (
            "/time-offs/holidays/",
            {"holidayGroupId": RUNN_HOLIDAY_GROUP_ID}
        )

    if not only_list:
        targets = collections
    else:
        targets = {k: collections[k] for k in only_list if k in collections}

    summary: Dict[str, int] = {}

    for tbl, spec in targets.items():
        if isinstance(spec, tuple):
            path, fixed_params = spec
        else:
            path, fixed_params = spec, None

        # ----- Construye parámetros dinámicos -----
        dyn_params: Dict[str, Optional[str]] = {}
        # Para actuals/assignments permite rango + persona
        if _accepts_date_window(path):
            if args.range_from and args.range_to:
                dyn_params["dateFrom"] = args.range_from
                dyn_params["dateTo"]   = args.range_to
            if args.person_id:
                dyn_params["personId"] = args.person_id

        # Mezcla fixed + dinámicos
        extra = dict(fixed_params or {})
        extra.update({k: v for k, v in dyn_params.items() if v})

        # ----- Decide filtro modifiedAfter (delta con solape) -----
        since_iso: Optional[str] = None
        use_modified_after = False
        if args.range_from and args.range_to and _accepts_date_window(path):
            # backfill dirigido: NO usamos modifiedAfter
            use_modified_after = False
        elif args.mode == "delta":
            last = get_last_success(bq, tbl)
            if last:
                overlap = dt.timedelta(days=max(args.overlap_days, 0))
                since = last - overlap
            else:
                since = now - dt.timedelta(days=args.delta_days)
            since_iso = since.strftime("%Y-%m-%dT%H:%M:%SZ")
            use_modified_after = _supports_modified_after(path)
        else:
            # full sin modifiedAfter
            use_modified_after = False

        # ----- Purga de ventana si procede (antes de descargar) -----
        if tbl in {"runn_actuals", "runn_assignments"} and (args.range_from and args.range_to):
            purge_scope(bq, tbl, args.person_id, args.range_from, args.range_to)

        # ----- Descarga -----
        rows = fetch_all(path, since_iso if use_modified_after else None, extra_params=extra)

        # ----- Carga/MERGE -----
        n = load_merge(tbl, rows, bq)
        summary[tbl] = int(n)

        # ----- Actualiza estado (checkpoint) -----
        # Regla:
        # - Si hubo filas: usa max(updatedAt) si existe; si no, usa now.
        # - Si no hubo filas:
        #     * en delta con modifiedAfter: NO muevas checkpoint (evita saltarte cambios tardíos).
        #     * en backfill (rango): sí podemos marcar now (opcional), pero no es necesario.
        if rows:
            # Busca updatedAt
            max_upd = None
            for r in rows:
                v = r.get("updatedAt") or r.get("updated_at")
                if v:
                    try:
                        # normaliza a datetime (asume formato ISOZ)
                        t = dt.datetime.fromisoformat(v.replace("Z","+00:00"))
                        if (max_upd is None) or (t > max_upd):
                            max_upd = t
                    except Exception:
                        pass
            set_last_success(bq, tbl, max_upd or now)
        else:
            if not use_modified_after:
                # en full/backfill sin modifiedAfter, podemos marcar now para no repetir,
                # pero igual lo dejamos sin mover. Comportamiento conservador:
                pass

    print(json.dumps({"ok": True, "loaded": summary}, ensure_ascii=False))

if __name__ == "__main__":
    main()
