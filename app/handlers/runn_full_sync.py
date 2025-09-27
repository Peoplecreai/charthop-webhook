# app/handlers/runn_full_sync.py
"""
Handler HTTP y CLI para disparar la exportación completa de Runn -> BigQuery.
- HTTP:  usado por /tasks/export-runn (Cloud Run/Flask).
- CLI:   python -m app.handlers.runn_full_sync  (usa env vars).
"""

from __future__ import annotations
import os
import logging
from typing import List, Optional, Dict, Any
from flask.typing import ResponseReturnValue  # no rompe en CLI, sólo tipado

# Importa el exportador real (ya escribe en BigQuery)
from app.services.runn_export_all import run_full_sync as _run_full_sync

log = logging.getLogger("runn-full-sync")
logging.basicConfig(level=logging.INFO)

def _parse_targets_from_env() -> Optional[List[str]]:
    raw = os.getenv("RUNN_EXPORT_TARGETS", "").strip()
    if not raw:
        return None
    return [t.strip() for t in raw.split(",") if t.strip()]

def _parse_window_days(default_days: int = 120, *, request=None) -> int:
    # Prioridad: query param (?window_days=) -> env WINDOW_DAYS -> default
    val = os.getenv("WINDOW_DAYS", str(default_days))
    try:
        days = int(val)
    except Exception:
        days = default_days
    if request is not None:
        try:
            q = request.args.get("window_days") or request.args.get("days")
            if q:
                days = int(q)
        except Exception:
            pass
    return days

# ===== HTTP (Cloud Run/Flask) =====
def export_handler(request=None) -> ResponseReturnValue:
    """
    Compatible con RUNN_EXPORT_HANDLER.
    Acepta ?window_days=120 y ?targets=people,projects,actuals,...
    """
    days = _parse_window_days(default_days=120, request=request)

    targets = None
    if request is not None:
        raw = (request.args.get("targets") or "").strip()
        if raw:
            targets = [t.strip() for t in raw.split(",") if t.strip()]
    if targets is None:
        targets = _parse_targets_from_env()

    log.info("Disparando export HTTP window_days=%s targets=%s", days, targets)
    result: Dict[str, Any] = _run_full_sync(window_days=days, targets=targets)
    return (result, 200)

# ===== CLI =====
if __name__ == "__main__":
    # Permite: RUNN_EXPORT_TARGETS="people,projects,actuals" WINDOW_DAYS=180 python -m app.handlers.runn_full_sync
    days = _parse_window_days(default_days=120, request=None)
    targets = _parse_targets_from_env()
    log.info("Disparando export CLI window_days=%s targets=%s", days, targets)
    out = _run_full_sync(window_days=days, targets=targets)
    # imprime el resumen para que veas fetched/inserted/merged por recurso
    print(out)
