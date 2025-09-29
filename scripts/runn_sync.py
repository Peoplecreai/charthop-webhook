from __future__ import annotations

import os
import sys
import time
import json
import argparse
import datetime as dt
from typing import Dict, List, Optional

import requests
from google.cloud import bigquery


# -----------------------------
# Configuración de API y BQ
# -----------------------------
API = "https://api.runn.io"

# Variables de entorno requeridas:
# - RUNN_API_TOKEN (Secret)
# - BQ_PROJECT
# - BQ_DATASET
try:
    PROJ = os.environ["BQ_PROJECT"]
    DS = os.environ["BQ_DATASET"]
    RUNN_TOKEN = os.environ["RUNN_API_TOKEN"]
except KeyError as e:
    missing = e.args[0]
    print(f"Falta variable de entorno requerida: {missing}", file=sys.stderr)
    sys.exit(2)

HDRS = {
    "Authorization": f"Bearer {RUNN_TOKEN}",
    "Accept-Version": "1.0.0",
    "Accept": "application/json",
}


# -----------------------------
# Endpoints a sincronizar
# -----------------------------
COLLS: Dict[str, str] = {
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
    # nuevos que pediste:
    "runn_contracts": "/contracts/",
    "runn_custom_fields": "/custom-fields/",
    "runn_holiday_groups": "/holiday-groups/",
    "runn_placeholders": "/placeholders/",
}


# -----------------------------
# Watermark de sincronización
# -----------------------------
def state_table() -> str:
    return f"{PROJ}.{DS}.__runn_sync_state"


def ensure_state_table(bq: bigquery.Client) -> None:
    bq.query(
        f"""
        CREATE TABLE IF NOT EXISTS `{state_table()}`(
          table_name STRING NOT NULL,
          last_success TIMESTAMP,
          PRIMARY KEY(table_name) NOT ENFORCED
        )
        """
    ).result()


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


def set_last_success(bq: bigquery.Client, name: str, ts: dt.datetime) -> None:
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
                bigquery.ScalarQueryParameter("t", "STRING", name),
                bigquery.ScalarQueryParameter("ts", "TIMESTAMP", ts.isoformat()),
            ]
        ),
    ).result()


# -----------------------------
# Extracción desde Runn (paginada)
# -----------------------------
def fetch_all(path: str, since_iso: Optional[str], limit: int = 200) -> List[dict]:
    s = requests.Session()
    s.headers.update(HDRS)
    out: List[dict] = []
    cursor: Optional[str] = None

    endpoint_key = path.rstrip("/").split("/")[-1]  # p.ej. "actuals"

    while True:
        params = {"limit": limit}
        # Sólo endpoints grandes soportados para delta por "modifiedAfter"
        if since_iso and endpoint_key in {"actuals", "assignments"}:
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


# -----------------------------
# Carga y MERGE en BigQuery
# -----------------------------
def load_merge(table_base: str, rows: List[dict], bq: bigquery.Client) -> int:
    """
    Sube a _stg__* con autodetección.
    Si hay columna 'id', hace MERGE T<->S con CAST a STRING en ON e INSERT.
    Si no hay 'id', reemplaza la tabla final.
    Devuelve filas en tabla final.
    """
    if not rows:
        return 0

    stg = f"{PROJ}.{DS}._stg__{table_base}"
    tgt = f"{PROJ}.{DS}.{table_base}"

    # 1) Carga a staging
    job = bq.load_table_from_json(
        rows,
        stg,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
        ),
    )
    job.result()

    stg_tbl = bq.get_table(stg)
    stg_schema = stg_tbl.schema

    # 2) Asegura existencia de target con el mismo esquema inicial
    try:
        bq.get_table(tgt)
    except Exception:
        bq.create_table(bigquery.Table(tgt, schema=stg_schema))

    # 3) MERGE seguro por id (CAST ambos lados a STRING)
    has_id = any(c.name == "id" for c in stg_schema)
    if has_id:
        cols = [c.name for c in stg_schema]
        non_id_cols = [c for c in cols if c != "id"]

        # SET columna a columna (sin tocar id)
        set_clause = ", ".join([f"T.{c}=S.{c}" for c in non_id_cols]) or "id = id"

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
        # Si no hay 'id', reemplazo completo
        sql = f"CREATE OR REPLACE TABLE `{tgt}` AS SELECT * FROM `{stg}`"

    bq.query(sql).result()
    return int(bq.get_table(tgt).num_rows)


# -----------------------------
# CLI
# -----------------------------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["full", "delta"], default="delta")
    ap.add_argument("--delta-days", type=int, default=90)
    # --only repetible o lista separada por comas; compatible hacia atrás
    ap.add_argument("--only", action="append", help="Repite el flag o pasa lista separada por comas.")
    # opcional: falla si todo devuelve 0 filas (útil en jobs programados)
    ap.add_argument("--fail-on-zero", action="store_true", help="Falla si ninguna colección cargó filas.")
    return ap.parse_args()


def build_targets(args_only: Optional[List[str]]) -> Dict[str, str]:
    if not args_only:
        return COLLS
    wanted: List[str] = []
    for item in args_only:
        wanted += [x.strip() for x in item.split(",") if x.strip()]
    # Validación explícita para evitar no-ops silenciosos
    missing = [k for k in wanted if k not in COLLS]
    if missing:
        raise SystemExit(f"--only contiene claves desconocidas: {missing}")
    return {k: COLLS[k] for k in wanted}


def main() -> None:
    args = parse_args()

    if os.getenv("RUNN_DEBUG") == "1":
        print("argv:", sys.argv)
        print("mode:", args.mode)
        print("delta-days:", args.delta_days)
        print("only_raw:", args.only)

    bq = bigquery.Client(project=PROJ)
    ensure_state_table(bq)

    now = dt.datetime.now(dt.timezone.utc)
    targets = build_targets(args.only)

    if os.getenv("RUNN_DEBUG") == "1":
        print("targets:", sorted(list(targets.keys())))

    summary: Dict[str, int] = {}
    any_rows = False

    for tbl, path in targets.items():
        # Calcular ventana delta si aplica
        since_iso: Optional[str] = None
        if args.mode == "delta":
            last = get_last_success(bq, tbl)
            if last:
                since_iso = last.strftime("%Y-%m-%dT%H:%M:%SZ")
            elif tbl in {"runn_actuals", "runn_assignments"}:
                since_iso = (now - dt.timedelta(days=args.delta_days)).strftime("%Y-%m-%dT%H:%M:%SZ")

        if os.getenv("RUNN_DEBUG") == "1":
            print(f"Fetching {tbl} from {path} (since={since_iso or 'FULL'})")

        rows = fetch_all(path, since_iso)
        loaded = load_merge(tbl, rows, bq)
        summary[tbl] = int(loaded)
        if loaded > 0:
            any_rows = True

        # Avanza watermark (timestamp de esta corrida)
        set_last_success(bq, tbl, now)

    print(json.dumps({"ok": True, "loaded": summary}, ensure_ascii=False))

    if args.fail_on_zero and not any_rows:
        # nada cargado: exit code 3 para que Scheduler/Alerting lo capte
        sys.exit(3)


if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as e:
        # Errores HTTP de la API de Runn
        try:
            payload = e.response.json()
        except Exception:
            payload = e.response.text
        print(json.dumps({"ok": False, "error": "runn_api", "status": e.response.status_code, "detail": payload}), file=sys.stderr)
        sys.exit(2)
    except Exception as e:
        # Cualquier otra excepción
        print(json.dumps({"ok": False, "error": "unexpected", "detail": str(e)}), file=sys.stderr)
        sys.exit(2)
