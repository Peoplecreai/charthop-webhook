# app/services/runn_export_all.py
# Exporta TODOS los recursos analíticamente útiles de Runn v1 hacia BigQuery.
# Respeta tus variables/secretos ya configurados.

import os
import time
import logging
from datetime import date, timedelta
from typing import Callable, Dict, List, Optional, Sequence, Tuple

import requests
import pandas as pd
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception, before_sleep_log
from google.cloud import bigquery


# ============ Config desde ENV ============
RUNN_API_BASE   = os.getenv("RUNN_API", "https://api.runn.io").rstrip("/")
RUNN_API_VER    = os.getenv("RUNN_API_VERSION", "1.0.0")
RUNN_API_TOKEN  = os.environ["RUNN_API_TOKEN"]                # secreto
HTTP_TIMEOUT    = int(os.getenv("HTTP_TIMEOUT", "30"))

BQ_PROJECT      = os.environ["BQ_PROJECT"]
BQ_DATASET      = os.environ["BQ_DATASET"]
BQ_LOCATION     = os.getenv("BQ_LOCATION", "US")

# Ventana por defecto para endpoints voluminosos (actuals, time-offs)
WINDOW_DAYS     = int(os.getenv("WINDOW_DAYS", "180"))

# Logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("runn-export")


# ============ HTTP helpers ============

class RunnError(Exception):
    pass

def _headers() -> Dict[str, str]:
    return {
        "accept": "application/json",
        "accept-version": RUNN_API_VER,
        "authorization": f"Bearer {RUNN_API_TOKEN}",
    }

def _url(path: str) -> str:
    if not path.startswith("/"):
        path = "/" + path
    return f"{RUNN_API_BASE}{path}"

class HTTPStatusError(RunnError):
    def __init__(self, status: int, msg: str, retry_after: Optional[int] = None):
        super().__init__(f"HTTP {status}: {msg}")
        self.status = status
        self.retry_after = retry_after

def _is_transient(exc: Exception) -> bool:
    if isinstance(exc, requests.RequestException):
        return True
    if isinstance(exc, HTTPStatusError):
        # Retriables típicos
        return exc.status in (408, 429, 500, 502, 503, 504)
    return False

@retry(
    wait=wait_exponential(multiplier=1, min=1, max=30),
    stop=stop_after_attempt(5),
    retry=retry_if_exception(_is_transient),
    before_sleep=before_sleep_log(log, logging.WARNING),
)
def _get(path: str, params: Optional[Dict[str, str]] = None) -> Dict:
    r = requests.get(_url(path), headers=_headers(), params=params, timeout=HTTP_TIMEOUT)

    # Éxito
    if 200 <= r.status_code < 300:
        return r.json() if r.text else {}

    # Errores transitorios (reintentar)
    if r.status_code in (408, 429, 500, 502, 503, 504):
        # Respeta Retry-After si está presente (segundos)
        ra = r.headers.get("Retry-After")
        if ra:
            try:
                time.sleep(int(ra))
            except Exception:
                pass
        raise HTTPStatusError(r.status_code, r.text[:500])

    # Errores no transitorios (4xx que no son 408/429)
    raise RunnError(f"GET {path} -> {r.status_code}: {r.text[:500]}")

def fetch_all(path: str, params: Optional[Dict[str, str]] = None) -> List[Dict]:
    """
    Recorre la paginación por cursor de Runn (campo 'nextCursor').
    Retorna la lista concatenada de 'values'.
    """
    params = dict(params or {})
    out: List[Dict] = []

    cursor: Optional[str] = None
    page = 1
    while True:
        if cursor:
            params["cursor"] = cursor
        data = _get(path, params=params)
        values = data.get("values", [])
        out.extend(values)

        cursor = data.get("nextCursor")
        log.info("GET %s page=%s total=%s", path, page, len(out))
        page += 1
        if not cursor:
            break
        # pequeño respiro para no saturar
        time.sleep(0.05)
    return out


# ============ BigQuery helpers ============

_bq_client: Optional[bigquery.Client] = None

def _client() -> bigquery.Client:
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=BQ_PROJECT, location=BQ_LOCATION)
    return _bq_client

def _table_id(table: str) -> str:
    return f"{BQ_PROJECT}.{BQ_DATASET}.{table}"

def _load_json(table: str, rows: List[Dict]):
    """
    Carga lista de JSON a BQ en formato NDJSON para evitar problemas con columnas object/JSON.
    WRITE_TRUNCATE para idempotencia. Loguea el resultado del job.
    """
    from io import BytesIO
    import json

    client = _client()
    table_id = _table_id(table)

    # Si no hay filas, asegura la tabla con un esquema mínimo
    if not rows:
        schema = [
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("createdAt", "TIMESTAMP"),
            bigquery.SchemaField("updatedAt", "TIMESTAMP"),
            bigquery.SchemaField("raw", "JSON"),
        ]
        try:
            client.get_table(table_id)
        except Exception:
            client.create_table(bigquery.Table(table_id, schema=schema))
            log.info("Tabla creada vacía: %s", table_id)
        return

    # Normaliza a DataFrame para forzar columnas básicas y luego arma NDJSON
    df = pd.json_normalize(rows, sep="_")
    for col in ("id", "createdAt", "updatedAt"):
        if col not in df.columns:
            df[col] = None

    # Convierte a NDJSON fila por fila
    buf = BytesIO()
    for i, r in df.iterrows():
        obj = r.dropna().to_dict()
        # Además guarda el JSON crudo por si quieres depurar o consultar campos no aplanados
        obj["raw"] = rows[i]
        line = json.dumps(obj, ensure_ascii=False)
        buf.write((line + "\n").encode("utf-8"))
    buf.seek(0)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
    )

    job = client.load_table_from_file(buf, table_id, job_config=job_config)
    result = job.result()  # espera y lanza si falla
    dest = client.get_table(table_id)
    log.info("CARGA %s -> %s filas_subidas=%s filas_en_tabla=%s estado=%s",
             table, table_id, dest.num_rows, dest.num_rows, result.state)


# ============ Exporters por recurso ============

def _window_params(days: int) -> Dict[str, str]:
    start = (date.today() - timedelta(days=days)).isoformat()
    # Los endpoints de Runn aceptan usualmente 'from' (y a veces 'to')
    return {"from": start}

def export_dimensions():
    """People / Projects / Clients / (opcional) Tags de personas/proyectos más abajo."""
    for spec in [
        ("/people",   "runn_people"),
        ("/projects", "runn_projects"),
        ("/clients",  "runn_clients"),
    ]:
        path, table = spec
        log.info("GET %s -> %s", path, table)
        rows = fetch_all(path)
        _load_json(table, rows)

def export_assignments(window_days: int = WINDOW_DAYS):
    # En assignments normalmente tiene sentido filtrar por ventana
    log.info("GET /assignments -> runn_assignments_raw")
    rows = fetch_all("/assignments", params=_window_params(window_days))
    _load_json("runn_assignments_raw", rows)

def export_actuals(window_days: int = WINDOW_DAYS):
    log.info("GET /actuals -> runn_actuals")
    rows = fetch_all("/actuals", params=_window_params(window_days))
    _load_json("runn_actuals", rows)

def export_time_offs(window_days: int = WINDOW_DAYS):
    for spec in [
        ("/time-offs/leave",    "runn_timeoffs_leave"),
        ("/time-offs/rostered", "runn_timeoffs_rostered"),
        ("/time-offs/holidays", "runn_timeoffs_holidays"),
    ]:
        path, table = spec
        log.info("GET %s -> %s", path, table)
        rows = fetch_all(path, params=_window_params(window_days))
        _load_json(table, rows)

def export_project_subresources():
    """
    Fase/milestones/notes/people por proyecto.
    """
    projects = fetch_all("/projects")
    ids = [p.get("id") for p in projects if p.get("id")]

    # phases
    all_rows = []
    for pid in ids:
        r = fetch_all(f"/projects/{pid}/phases")
        for x in r:
            x["_projectId"] = pid
        all_rows.extend(r)
    _load_json("runn_phases", all_rows)

    # milestones
    all_rows = []
    for pid in ids:
        r = fetch_all(f"/projects/{pid}/milestones")
        for x in r:
            x["_projectId"] = pid
        all_rows.extend(r)
    _load_json("runn_milestones", all_rows)

    # notes
    all_rows = []
    for pid in ids:
        r = fetch_all(f"/projects/{pid}/notes")
        for x in r:
            x["_projectId"] = pid
        all_rows.extend(r)
    _load_json("runn_notes", all_rows)

    # people on project
    all_rows = []
    for pid in ids:
        r = fetch_all(f"/projects/{pid}/people")
        for x in r:
            x["_projectId"] = pid
        all_rows.extend(r)
    _load_json("runn_people_on_project", all_rows)

    # assignments por proyecto (vista de conveniencia)
    all_rows = []
    for pid in ids:
        r = fetch_all(f"/projects/{pid}/assignments")
        for x in r:
            x["_projectId"] = pid
        all_rows.extend(r)
    _load_json("runn_project_assignments", all_rows)

    # actuals por proyecto (si quieres una tabla separada)
    all_rows = []
    for pid in ids:
        r = fetch_all(f"/projects/{pid}/actuals")
        for x in r:
            x["_projectId"] = pid
        all_rows.extend(r)
    _load_json("runn_project_actuals", all_rows)

def export_holidays_detail():
    """Alias simple si necesitas tabla 'runn_holidays' a partir de time-offs/holidays."""
    rows = fetch_all("/time-offs/holidays")
    _load_json("runn_holidays", rows)


# ===== Nuevos recursos generales (lo que faltaba) =====

NEW_RESOURCE_SPECS = {
    "roles":         {"path": "/roles",          "table": "runn_roles"},
    "teams":         {"path": "/teams",          "table": "runn_teams"},
    "skills":        {"path": "/skills",         "table": "runn_skills"},
    "users":         {"path": "/users",          "table": "runn_users"},
    "placeholders":  {"path": "/placeholders",   "table": "runn_placeholders"},
    "people_tags":   {"path": "/people-tags",    "table": "runn_people_tags"},
    "project_tags":  {"path": "/project-tags",   "table": "runn_project_tags"},
    "workstreams":   {"path": "/workstreams",    "table": "runn_workstreams"},
    "contracts":     {"path": "/contracts",      "table": "runn_contracts"},
    "custom_fields": {"path": "/custom-fields",  "table": "runn_custom_fields"},
    # Rate cards globales
    "rate_cards":    {"path": "/rate-cards",     "table": "runn_rate_cards"},
}

def export_new_resources():
    client = _client()  # inicializa
    for key, spec in NEW_RESOURCE_SPECS.items():
        path = spec["path"]; table = spec["table"]
        log.info("GET %s -> %s", path, table)
        rows = fetch_all(path)
        _load_json(table, rows)

def export_project_rate_cards():
    """
    Rate cards por proyecto (subrecurso). Crea tabla runn_project_rate_cards.
    """
    log.info("GET /projects/*/rate-cards -> runn_project_rate_cards")
    projects = fetch_all("/projects")
    ids = [p.get("id") for p in projects if p.get("id")]
    all_rows: List[Dict] = []

    for pid in ids:
        r = fetch_all(f"/projects/{pid}/rate-cards")
        for x in r:
            x["_projectId"] = pid
        all_rows.extend(r)

    _load_json("runn_project_rate_cards", all_rows)


# ============ Orquestación ============

def run_full_sync(
    window_days: Optional[int] = None,
    targets: Optional[Sequence[str]] = None,
) -> Dict[str, object]:
    """
    Orquesta la exportación total. Llama a TODO lo relevante.

    Args:
        window_days: Ventana a utilizar para recursos filtrables. Si es ``None``
            usa :data:`WINDOW_DAYS`.
        targets: Lista opcional de secciones a ejecutar. Si se deja vacío o
            contiene ``"all"`` se ejecutan todas.

    Returns:
        Un resumen con los targets ejecutados.
    """

    w = int(window_days if window_days is not None else WINDOW_DAYS)
    normalized: List[str] = []
    if targets:
        for t in targets:
            if not t:
                continue
            key = t.strip().lower()
            if key:
                normalized.append(key)

    # Mantener orden lógico de exportación
    steps: List[Tuple[str, Callable[[], None]]] = [
        ("dimensions", export_dimensions),
        ("assignments", lambda: export_assignments(w)),
        ("actuals", lambda: export_actuals(w)),
        ("time_offs", lambda: export_time_offs(w)),
        ("project_subresources", export_project_subresources),
        ("holidays", export_holidays_detail),
        ("new_resources", export_new_resources),
        ("project_rate_cards", export_project_rate_cards),
    ]

    # Convertir a mapa de funciones llamables
    ordered_targets = [name for name, _ in steps]
    valid_names = set(ordered_targets)
    requested_all = not normalized or any(t in ("all", "*") for t in normalized)
    requested_set = valid_names if requested_all else {t for t in normalized if t in valid_names}
    unknown = [t for t in normalized if t not in valid_names and t not in ("all", "*")]

    executed: List[str] = []

    log.info("=== RUNN FULL SYNC (window_days=%s targets=%s) ===", w, normalized or "all")

    for name, fn in steps:
        if requested_all or name in requested_set:
            log.info("-> Export %s", name)
            fn()
            executed.append(name)
        else:
            log.info("-> Skip %s", name)

    if unknown:
        log.warning("Targets no reconocidos: %s", unknown)

    log.info("=== RUNN FULL SYNC DONE (executed=%s) ===", executed)
    skipped = [name for name in ordered_targets if name not in executed]
    return {"window_days": w, "executed": executed, "skipped": skipped, "unknown": unknown}
