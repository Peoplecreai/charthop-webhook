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
# =============================================================================
# CAMBIO FORZADO: Ignoramos la variable de entorno y usamos el valor correcto.
# =============================================================================
DS   = "people_analytics" 

# Imprimimos los valores para estar 100% seguros de lo que se está usando
print(f"DEBUG: Usando proyecto BQ '{PROJ}' y dataset '{DS}'")
# =============================================================================

# Filtro opcional para holidays (si lo defines en el Job limitará el volumen)
RUNN_HOLIDAY_GROUP_ID = os.environ.get("RUNN_HOLIDAY_GROUP_ID")

# -----------------------------------------------------------------------------
# Catálogo de colecciones.
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

# -----------------------------------------------------------------------------
# Esquemas destino "estables" (evita autodetecciones inconsistentes)
# Solo declaramos donde nos importa fijar tipos.
# -----------------------------------------------------------------------------
SCHEMA_OVERRIDES: Dict[str, List[bigquery.SchemaField]] = {
    "runn_actuals": [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("billableMinutes", "INT64"),
        bigquery.SchemaField("nonbillableMinutes", "INT64"),
        bigquery.SchemaField("phaseId", "INT64"),
        bigquery.SchemaField("projectId", "INT64"),
        bigquery.SchemaField("personId", "INT64"),
        bigquery.SchemaField("roleId", "INT64"),
        bigquery.SchemaField("workstreamId", "STRING"),
        bigquery.SchemaField("updatedAt", "TIMESTAMP"),
        bigquery.SchemaField("createdAt", "TIMESTAMP"),
    ],
    "runn_timeoffs_leave": [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("personId", "STRING"),
        bigquery.SchemaField("startDate", "DATE"),
        bigquery.SchemaField("endDate", "DATE"),
        bigquery.SchemaField("note", "STRING"),
        bigquery.SchemaField("createdAt", "TIMESTAMP"),
        bigquery.SchemaField("updatedAt", "TIMESTAMP"),
        bigquery.SchemaField("minutesPerDay", "INT64"),
    ],
    "runn_timeoffs_rostered": [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("personId", "STRING"),
        bigquery.SchemaField("startDate", "DATE"),
        bigquery.SchemaField("endDate", "DATE"),
        bigquery.SchemaField("note", "STRING"),
        bigquery.SchemaField("createdAt", "TIMESTAMP"),
        bigquery.SchemaField("updatedAt", "TIMESTAMP"),
        bigquery.SchemaField("minutesPerDay", "INT64"),
    ],
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
# Helpers de endpoint
# -----------------------
def _supports_modified_after(path: str) -> bool:
    tail = path.rstrip("/").split("/")[-1]
    return tail in {"actuals", "assignments", "contracts", "placeholders"}

def _accepts_date_window(path: str) -> bool:
    tail = path.rstrip("/").split("/")[-1]
    return tail in {"actuals", "assignments"}

# -----------------------
# Descarga paginada
# -----------------------
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
        print(f"[INFO] fetched {len(out)} rows from {path} (params={extra_params or {}} since={since_iso})")
    return out

# -----------------------
# BQ: helpers
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
    schema = SCHEMA_OVERRIDES[table_base]
    bq.create_table(bigquery.Table(tgt, schema=schema))

def _schema_to_map(schema: List[bigquery.SchemaField]) -> Dict[str, str]:
    return {c.name: c.field_type.upper() for c in schema}

def _cast_expr(col: str, bq_type: str) -> str:
    # id siempre como STRING para clave de MERGE
    if col == "id":
        return "CAST(id AS STRING) AS id"
    t = bq_type.upper()
    if t in {"STRING"}:
        return f"CAST({col} AS STRING) AS {col}"
    if t in {"INT64", "INTEGER"}:
        return f"SAFE_CAST({col} AS INT64) AS {col}"
    if t in {"FLOAT64", "FLOAT"}:
        return f"SAFE_CAST({col} AS FLOAT64) AS {col}"
    if t in {"BOOL", "BOOLEAN"}:
        return f"SAFE_CAST({col} AS BOOL) AS {col}"
    if t == "DATE":
        return f"SAFE_CAST({col} AS DATE) AS {col}"
    if t == "TIMESTAMP":
        return f"SAFE_CAST({col} AS TIMESTAMP) AS {col}"
    if t == "DATETIME":
        return f"SAFE_CAST({col} AS DATETIME) AS {col}"
    # por defecto sin cast
    return f"{col} AS {col}"

def _ensure_target_table(table_base: str, stg_schema: List[bigquery.SchemaField], bq: bigquery.Client) -> List[bigquery.SchemaField]:
    """
    Crea la tabla destino si no existe. Si hay override conocido, úsalo.
    Devuelve el schema efectivo de destino.
    """
    tgt_id = f"{PROJ}.{DS}.{table_base}"
    try:
        tgt_tbl = bq.get_table(tgt_id)
        return tgt_tbl.schema
    except Exception:
        pass

    if table_base in SCHEMA_OVERRIDES:
        schema = SCHEMA_OVERRIDES[table_base]
        # particiona si hay columna date
        has_date = any(c.name == "date" and c.field_type.upper()=="DATE" for c in schema)
        if has_date:
            q = f"""
            CREATE TABLE `{tgt_id}`
            PARTITION BY DATE(date)
            CLUSTER BY personId, projectId
            AS SELECT * FROM `{PROJ}.{DS}._stg__{table_base}` WHERE 1=0
            """
            # Creamos con DDL; luego ajustamos el esquema por exactitud
            bq.query(q).result()
            # Re-define esquema explícito (evita heredar autodetección)
            bq.update_table(bigquery.Table(tgt_id, schema=schema), ["schema"])
            return schema
        else:
            bq.create_table(bigquery.Table(tgt_id, schema=schema))
            return schema
    else:
        # default: clona esquema de staging
        bq.create_table(bigquery.Table(tgt_id, schema=stg_schema))
        return stg_schema

# -----------------------
# Carga y MERGE a BQ (tipado contra destino)
# -----------------------
def load_merge(table_base: str, rows: List[Dict], bq: bigquery.Client) -> int:
    if not rows:
        _create_empty_timeoff_table_if_needed(table_base, bq)
        return 0

    stg = f"{PROJ}.{DS}._stg__{table_base}"
    tgt = f"{PROJ}.{DS}.{table_base}"

    # 1) Carga staging
    job = bq.load_table_from_json(
        rows,
        stg,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True
        )
    )
    job.result()

    stg_schema = bq.get_table(stg).schema
    # 2) Asegura destino (usa overrides si hay)
    tgt_schema = _ensure_target_table(table_base, stg_schema, bq)

    # 3) Construye SELECT tipado (S) para alinear a esquema destino
    tgt_map = _schema_to_map(tgt_schema)
    stg_cols = [c.name for c in stg_schema]

    select_parts: List[str] = []
    for col, bq_type in tgt_map.items():
        if col in stg_cols:
            select_parts.append(_cast_expr(col, bq_type))
        else:
            # columna está en destino pero no vino en staging -> NULL tipado
            select_parts.append(f"CAST(NULL AS {bq_type}) AS {col}")
    select_sql = ",\\n    ".join(select_parts)

    # columnas para MERGE (intersección menos id)
    non_id_cols = [c for c in tgt_map.keys() if c != "id" and c in stg_cols]

    set_clause  = ", ".join([f"T.{c}=S.{c}" for c in non_id_cols]) if non_id_cols else ""
    insert_cols = ["id"] + [c for c in tgt_map.keys() if c != "id" and c in stg_cols]
    insert_vals = ["S.id"] + [f"S.{c}" for c in insert_cols if c != "id"]

    merge_sql = f"""
    MERGE `{tgt}` T
    USING (
      SELECT
        {select_sql}
      FROM `{stg}`
    ) S
    ON CAST(T.id AS STRING) = S.id
    """

    if set_clause:
        merge_sql += f"""
    WHEN MATCHED THEN UPDATE SET
      {set_clause}
    """

    merge_sql += f"""
    WHEN NOT MATCHED THEN INSERT ({", ".join(insert_cols)})
    VALUES ({", ".join(insert_vals)})
    """

    bq.query(merge_sql).result()
    return bq.get_table(tgt).num_rows

# -----------------------
# Purga de ventana (para backfill por rango – sin personas hardcode)
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

    # Backfill dirigido (rango global por fechas; persona opcional, NO hardcode)
    ap.add_argument("--range-from", dest="range_from", help="YYYY-MM-DD")
    ap.add_argument("--range-to",   dest="range_to",   help="YYYY-MM-DD")
    ap.add_argument("--person-id",  dest="person_id",  help="filtrar por persona (opcional)")

    args = ap.parse_args()
    only_list = parse_only(args.only)

    bq = bigquery.Client(project=PROJ)
    ensure_state_table(bq)

    now = dt.datetime.now(dt.timezone.utc)

    # Filtro de holiday group, si aplica
    collections: Dict[str, Union[str, Tuple[str, Dict[str, str]]]] = dict(COLLS)
    if RUNN_HOLIDAY_GROUP_ID:
        collections["runn_timeoffs_holidays"] = (
            "/time-offs/holidays/",
            {"holidayGroupId": RUNN_HOLIDAY_GROUP_ID}
        )

    targets = collections if not only_list else {k: collections[k] for k in only_list if k in collections}
    summary: Dict[str, int] = {}

    for tbl, spec in targets.items():
        path, fixed_params = (spec if isinstance(spec, tuple) else (spec, None))

        last_checkpoint = get_last_success(bq, tbl)

        # ----- Parámetros dinámicos (fecha/persona, sin hardcodeos) -----
        dyn_params: Dict[str, Optional[str]] = {}
        purge_from: Optional[str] = None
        purge_to: Optional[str] = None

        if _accepts_date_window(path):
            if args.range_from and args.range_to:
                dyn_params["dateFrom"] = args.range_from
                dyn_params["dateTo"]   = args.range_to
                purge_from, purge_to = args.range_from, args.range_to
            if args.person_id:
                dyn_params["personId"] = args.person_id

            if (args.mode == "delta") and not (args.range_from and args.range_to):
                window_days = max(args.delta_days, args.overlap_days, 0)
                start_date = (now - dt.timedelta(days=window_days)).date().isoformat()
                # no asumimos futuro; si RUNN entregara futuros, puedes ajustar aquí
                end_date = now.date().isoformat()
                dyn_params.setdefault("dateFrom", start_date)
                dyn_params.setdefault("dateTo", end_date)
                purge_from = dyn_params.get("dateFrom")
                purge_to   = dyn_params.get("dateTo")

        extra = dict(fixed_params or {})
        extra.update({k: v for k, v in dyn_params.items() if v})

        # ----- Strategy modifiedAfter vs ventana -----
        since_iso: Optional[str] = None
        use_modified_after = False
        if (args.range_from and args.range_to) and _accepts_date_window(path):
            use_modified_after = False
        elif purge_from and purge_to and _accepts_date_window(path):
            use_modified_after = False
        elif args.mode == "delta":
            if last_checkpoint:
                overlap = dt.timedelta(days=max(args.overlap_days, 0))
                since = last_checkpoint - overlap
            else:
                since = now - dt.timedelta(days=args.delta_days)
            since_iso = since.strftime("%Y-%m-%dT%H:%M:%SZ")
            use_modified_after = _supports_modified_after(path)

        # ----- Purga de ventana (si procede) -----
        if tbl in {"runn_actuals", "runn_assignments"} and purge_from and purge_to:
            purge_scope(bq, tbl, args.person_id, purge_from, purge_to)

        # ----- Descarga -----
        rows = fetch_all(path, since_iso if use_modified_after else None, extra_params=extra)

        # ----- Carga/MERGE -----
        n = load_merge(tbl, rows, bq)
        summary[tbl] = int(n)

        # ----- Checkpoint -----
        if rows:
            max_upd = None
            for r in rows:
                v = r.get("updatedAt") or r.get("updated_at")
                if v:
                    try:
                        t = dt.datetime.fromisoformat(v.replace("Z","+00:00"))
                        max_upd = t if (max_upd is None or t > max_upd) else max_upd
                    except Exception:
                        pass
            new_checkpoint = max_upd or now
            if last_checkpoint and new_checkpoint < last_checkpoint:
                new_checkpoint = last_checkpoint
            set_last_success(bq, tbl, new_checkpoint)
        # si no hubo filas en delta, no movemos el checkpoint (conservador)

    print(json.dumps({"ok": True, "loaded": summary}, ensure_ascii=False))

if __name__ == "__main__":
    main()
