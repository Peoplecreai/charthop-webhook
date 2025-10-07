from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import time
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union

import requests
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField


API = "https://api.runn.io"
HDRS = {
    "Authorization": f"Bearer {os.environ['RUNN_API_TOKEN']}",
    "Accept-Version": "1.0.0",
    "Accept": "application/json",
}
PROJ = os.environ.get("BQ_PROJECT", "integration-hub-468417")
DS = os.environ["BQ_DATASET"]
LOCATION = os.environ.get("BIGQUERY_LOCATION", "US")

# Filtro opcional para holidays (si lo defines en el Job limitará el volumen)
RUNN_HOLIDAY_GROUP_ID = os.environ.get("RUNN_HOLIDAY_GROUP_ID")


COLLS: Dict[str, Union[str, Tuple[str, Dict[str, str]]]] = {
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
    "runn_holiday_groups": "/holiday-groups/",
    "runn_placeholders": ("/placeholders/", {}),
    "runn_contracts": ("/contracts/", {"sortBy": "id"}),
    "runn_custom_fields_checkbox_person": ("/custom-fields/checkbox/", {"model": "PERSON"}),
    "runn_custom_fields_checkbox_project": ("/custom-fields/checkbox/", {"model": "PROJECT"}),
}


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


def state_table() -> str:
    return f"{PROJ}.{DS}.__runn_sync_state"


def ensure_state_table(bq: bigquery.Client) -> None:
    bq.query(
        f"""
    CREATE TABLE IF NOT EXISTS `{state_table()}`(
      table_name STRING NOT NULL,
      last_success TIMESTAMP,
      PRIMARY KEY(table_name) NOT ENFORCED
    )"""
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


def _supports_modified_after(path: str) -> bool:
    tail = path.rstrip("/").split("/")[-1]
    return tail in {"actuals", "assignments", "contracts", "placeholders"}


def _accepts_date_window(path: str) -> bool:
    tail = path.rstrip("/").split("/")[-1]
    return tail in {"actuals", "assignments"}


def fetch_all(
    path: str,
    since_iso: Optional[str],
    *,
    limit: int = 200,
    extra_params: Optional[Dict[str, str]] = None,
) -> List[Dict]:
    session = requests.Session()
    session.headers.update(HDRS)
    out: List[Dict] = []
    cursor: Optional[str] = None

    while True:
        params: Dict[str, str] = {"limit": str(limit)}
        if extra_params:
            params.update(
                {
                    k: v
                    for k, v in extra_params.items()
                    if v is not None and v != ""
                }
            )
        if since_iso and _supports_modified_after(path):
            params["modifiedAfter"] = since_iso
        if cursor:
            params["cursor"] = cursor

        response = session.get(API + path, params=params, timeout=60)
        if response.status_code == 429:
            retry = int(response.headers.get("Retry-After", "5") or "5")
            time.sleep(retry)
            continue
        if response.status_code == 404:
            if os.environ.get("RUNN_DEBUG"):
                print(
                    f"[WARN] 404 Not Found: {API+path} params={params}  (ignorado)"
                )
            return []
        response.raise_for_status()

        payload = response.json()
        values = payload.get("values", payload if isinstance(payload, list) else [])
        if isinstance(values, dict):
            values = [values]
        out.extend(values)

        cursor = payload.get("nextCursor")
        if not cursor:
            break

    if os.environ.get("RUNN_DEBUG"):
        print(
            f"[INFO] fetched {len(out)} rows from {path} (params={extra_params or {}} since={since_iso})"
        )
    return out


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


def _schema_to_map(schema: Sequence[bigquery.SchemaField]) -> Dict[str, str]:
    return {c.name: c.field_type.upper() for c in schema}


def _schema_is_repeated(f: SchemaField) -> bool:
    return ((getattr(f, "mode", None) or "").upper() == "REPEATED")


def _collect_repeated_columns(schema: Sequence[SchemaField]) -> Set[str]:
    return {f.name for f in schema if _schema_is_repeated(f)}


def _cast_expr(col: str, bq_type: str) -> str:
    if col == "id":
        return "CAST(id AS STRING) AS id"
    t = bq_type.upper()
    if t == "STRING":
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
    return f"{col} AS {col}"


def _ensure_target_table(
    table_base: str, stg_schema: Sequence[bigquery.SchemaField], bq: bigquery.Client
) -> Sequence[bigquery.SchemaField]:
    tgt_id = f"{PROJ}.{DS}.{table_base}"
    try:
        tgt_tbl = bq.get_table(tgt_id)
        return tgt_tbl.schema
    except Exception:
        pass

    if table_base in SCHEMA_OVERRIDES:
        schema = SCHEMA_OVERRIDES[table_base]
        has_date = any(c.name == "date" and c.field_type.upper() == "DATE" for c in schema)
        if has_date:
            query = f"""
            CREATE TABLE `{tgt_id}`
            PARTITION BY DATE(date)
            CLUSTER BY personId, projectId
            AS SELECT * FROM `{PROJ}.{DS}._stg__{table_base}` WHERE 1=0
            """
            bq.query(query).result()
            bq.update_table(bigquery.Table(tgt_id, schema=schema), ["schema"])
            return schema
        bq.create_table(bigquery.Table(tgt_id, schema=schema))
        return schema
    bq.create_table(bigquery.Table(tgt_id, schema=stg_schema))
    return stg_schema


def load_merge(table_base: str, rows: List[Dict], bq: bigquery.Client) -> int:
    if not rows:
        _create_empty_timeoff_table_if_needed(table_base, bq)
        return 0

    stg = f"{PROJ}.{DS}._stg__{table_base}"
    tgt = f"{PROJ}.{DS}.{table_base}"

    job = bq.load_table_from_json(
        rows,
        stg,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True),
    )
    job.result()

    stg_schema = bq.get_table(stg).schema
    tgt_schema = _ensure_target_table(table_base, stg_schema, bq)

    tgt_map = _schema_to_map(tgt_schema)
    stg_cols = {c.name for c in stg_schema}
    stg_repeated = _collect_repeated_columns(stg_schema)

    select_parts: List[str] = []
    for col, bq_type in tgt_map.items():
        if col in stg_cols:
            if col in stg_repeated and bq_type.upper() == "STRING":
                select_parts.append(f"{col}[SAFE_OFFSET(0)] AS {col}")
            else:
                select_parts.append(_cast_expr(col, bq_type))
        else:
            select_parts.append(f"CAST(NULL AS {bq_type}) AS {col}")
    select_clause = ",\n  ".join(select_parts)

    non_id_cols = [c for c in tgt_map.keys() if c != "id" and c in stg_cols]
    set_clause = ", ".join([f"T.{c}=S.{c}" for c in non_id_cols]) if non_id_cols else ""
    insert_cols = ["id"] + [c for c in tgt_map.keys() if c != "id" and c in stg_cols]
    insert_vals = ["S.id"] + [f"S.{c}" for c in insert_cols if c != "id"]

    merge_sql = f"""
    MERGE `{tgt}` T
    USING (
      SELECT
        {select_clause}
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

    print("[DEBUG] MERGE SQL:\n" + merge_sql, flush=True)
    bq.query(merge_sql).result()
    return bq.get_table(tgt).num_rows


def purge_scope(
    bq: bigquery.Client,
    table_base: str,
    person: Optional[str],
    dfrom: Optional[str],
    dto: Optional[str],
) -> None:
    if not (dfrom and dto):
        return
    if table_base not in {"runn_actuals", "runn_assignments"}:
        return

    if person:
        query = f"""
        DELETE FROM `{PROJ}.{DS}.{table_base}`
        WHERE CAST(personId AS STRING)=@p
          AND DATE(date) BETWEEN @d1 AND @d2
        """
        params = [
            bigquery.ScalarQueryParameter("p", "STRING", person),
            bigquery.ScalarQueryParameter("d1", "DATE", dfrom),
            bigquery.ScalarQueryParameter("d2", "DATE", dto),
        ]
    else:
        query = f"""
        DELETE FROM `{PROJ}.{DS}.{table_base}`
        WHERE DATE(date) BETWEEN @d1 AND @d2
        """
        params = [
            bigquery.ScalarQueryParameter("d1", "DATE", dfrom),
            bigquery.ScalarQueryParameter("d2", "DATE", dto),
        ]
    bq.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()


def parse_only(raw: Optional[Iterable[str]]) -> Optional[List[str]]:
    if not raw:
        return None
    out: List[str] = []
    for item in raw:
        out.extend([p.strip() for p in item.split(",") if p.strip()])
    seen = set()
    ordered: List[str] = []
    for key in out:
        if key not in seen:
            seen.add(key)
            ordered.append(key)
    return ordered


def run_sync(
    *,
    mode: str = "delta",
    delta_days: int = 60,
    overlap_days: int = 7,
    only: Optional[Sequence[str]] = None,
    range_from: Optional[str] = None,
    range_to: Optional[str] = None,
    person_id: Optional[str] = None,
) -> Dict[str, Dict[str, int]]:
    only_list = parse_only(only)

    bq = bigquery.Client(project=PROJ, location=LOCATION)
    ensure_state_table(bq)

    now = dt.datetime.now(dt.timezone.utc)

    collections: Dict[str, Union[str, Tuple[str, Dict[str, str]]]] = dict(COLLS)
    if RUNN_HOLIDAY_GROUP_ID:
        collections["runn_timeoffs_holidays"] = (
            "/time-offs/holidays/",
            {"holidayGroupId": RUNN_HOLIDAY_GROUP_ID},
        )

    targets = (
        collections
        if not only_list
        else {k: collections[k] for k in only_list if k in collections}
    )
    summary: Dict[str, int] = {}

    delta_days = max(delta_days, 0)
    overlap_days = max(overlap_days, 0)

    for tbl, spec in targets.items():
        path, fixed_params = (spec if isinstance(spec, tuple) else (spec, None))

        last_checkpoint = get_last_success(bq, tbl)

        dyn_params: Dict[str, Optional[str]] = {}
        purge_from: Optional[str] = None
        purge_to: Optional[str] = None

        if _accepts_date_window(path):
            if range_from and range_to:
                dyn_params["dateFrom"] = range_from
                dyn_params["dateTo"] = range_to
                purge_from, purge_to = range_from, range_to
            if person_id:
                dyn_params["personId"] = person_id

            if (mode == "delta") and not (range_from and range_to):
                window_days = max(delta_days, overlap_days)
                start_date = (now - dt.timedelta(days=window_days)).date().isoformat()
                end_date = now.date().isoformat()
                dyn_params.setdefault("dateFrom", start_date)
                dyn_params.setdefault("dateTo", end_date)
                purge_from = dyn_params.get("dateFrom")
                purge_to = dyn_params.get("dateTo")

        extra = dict(fixed_params or {})
        extra.update({k: v for k, v in dyn_params.items() if v})

        since_iso: Optional[str] = None
        use_modified_after = False
        if (range_from and range_to) and _accepts_date_window(path):
            use_modified_after = False
        elif purge_from and purge_to and _accepts_date_window(path):
            use_modified_after = False
        elif mode == "delta":
            if last_checkpoint:
                overlap = dt.timedelta(days=overlap_days)
                since = last_checkpoint - overlap
            else:
                since = now - dt.timedelta(days=delta_days)
            since_iso = since.strftime("%Y-%m-%dT%H:%M:%SZ")
            use_modified_after = _supports_modified_after(path)

        if tbl in {"runn_actuals", "runn_assignments"} and purge_from and purge_to:
            purge_scope(bq, tbl, person_id, purge_from, purge_to)

        rows = fetch_all(path, since_iso if use_modified_after else None, extra_params=extra)

        num_rows = load_merge(tbl, rows, bq)
        summary[tbl] = int(num_rows)

        if rows:
            max_upd = None
            for entry in rows:
                value = entry.get("updatedAt") or entry.get("updated_at")
                if not value:
                    continue
                try:
                    ts = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
                except Exception:
                    continue
                if not max_upd or ts > max_upd:
                    max_upd = ts
            new_checkpoint = max_upd or now
            if last_checkpoint and new_checkpoint < last_checkpoint:
                new_checkpoint = last_checkpoint
            set_last_success(bq, tbl, new_checkpoint)

    return {"ok": True, "loaded": summary}


def export_runn_snapshot(window_days: int = 60) -> Dict[str, Dict[str, int]]:
    overlap = min(7, window_days) if window_days > 0 else 0
    return run_sync(mode="delta", delta_days=window_days, overlap_days=overlap)


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["full", "delta"], default="delta")
    ap.add_argument("--delta-days", type=int, default=60)
    ap.add_argument(
        "--overlap-days",
        type=int,
        default=7,
        help="relee últimos N días en delta",
    )
    ap.add_argument(
        "--only",
        action="append",
        help="repetible o coma-separado: runn_people,runn_projects,…",
    )
    ap.add_argument("--range-from", dest="range_from", help="YYYY-MM-DD")
    ap.add_argument("--range-to", dest="range_to", help="YYYY-MM-DD")
    ap.add_argument("--person-id", dest="person_id", help="filtrar por persona (opcional)")
    return ap


def cli_main(argv: Optional[Sequence[str]] = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    result = run_sync(
        mode=args.mode,
        delta_days=args.delta_days,
        overlap_days=args.overlap_days,
        only=args.only,
        range_from=args.range_from,
        range_to=args.range_to,
        person_id=args.person_id,
    )
    print(json.dumps(result, ensure_ascii=False))


__all__ = [
    "export_runn_snapshot",
    "run_sync",
    "cli_main",
    "build_arg_parser",
]

