# app/handlers/runn_full_sync.py
"""
HTTP y CLI para exportar Runn -> BigQuery.
- HTTP: usado por /tasks/export-runn
- CLI:  python -m app.handlers.runn_full_sync
"""
from __future__ import annotations
import os, logging
from typing import List, Optional, Dict, Any
from flask.typing import ResponseReturnValue

# Importa el export real
from app.services.runn_export_all import run_full_sync as _run_full_sync

log = logging.getLogger("runn-full-sync")
logging.basicConfig(level=logging.INFO)

def _parse_targets_from_env() -> Optional[List[str]]:
    raw = os.getenv("RUNN_EXPORT_TARGETS", "").strip()
    if not raw:
        return None
    return [t.strip() for t in raw.split(",") if t.strip()]

def _parse_window_days(default_days: int = 180, *, request=None) -> int:
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

def _invoke_export(days: int, targets: Optional[List[str]]) -> Optional[Dict[str, Any]]:
    """
    Soporta ambas firmas de run_full_sync:
      - run_full_sync(days: int)
      - run_full_sync(window_days: Optional[int]=..., targets: Optional[List[str]]=...)
    """
    try:
        # Preferir kwargs si existen
        return _run_full_sync(window_days=days, targets=targets)  # type: ignore[call-arg]
    except TypeError as exc:
        # Firma anterior (posicional)
        if "unexpected keyword" not in str(exc):
            raise
        return _run_full_sync(days)  # type: ignore[misc]

def export_handler(request=None) -> ResponseReturnValue:
    days = _parse_window_days(default_days=180, request=request)
    targets = None
    if request is not None:
        raw = (request.args.get("targets") or "").strip()
        if raw:
            targets = [t.strip() for t in raw.split(",") if t.strip()]
    if targets is None:
        targets = _parse_targets_from_env()

    log.info("Export window_days=%s targets=%s", days, targets)
    result = _invoke_export(days, targets)
    body: Dict[str, Any] = {"status": "ok", **(result or {})}
    return (body, 200)

if __name__ == "__main__":
    days = _parse_window_days(default_days=180, request=None)
    targets = _parse_targets_from_env()
    log.info("CLI export window_days=%s targets=%s", days, targets)
    out = _invoke_export(days, targets)
    print(out)
