"""Helpers to export Runn data snapshots.

The actual export logic lives outside this repository and can be plugged in at
runtime via the ``RUNN_EXPORT_HANDLER`` environment variable.  We keep the
integration flexible because the snapshot pipeline is handled in different ways
per environment (for example, writing to BigQuery or exporting CSVs).

If ``RUNN_EXPORT_HANDLER`` is not configured the module will simply return a
status payload so the Cloud Run service keeps responding instead of failing to
import at start-up.
"""
from __future__ import annotations

from importlib import import_module
import logging
import os
from typing import Any, Callable, Dict, Optional

Handler = Callable[..., Dict[str, Any]]

_cached_handler: Optional[Handler] = None
_handler_attempted: bool = False


def _resolve_handler() -> Optional[Handler]:
    """Lazily resolve the export handler defined by ``RUNN_EXPORT_HANDLER``.

    The environment variable should have the format ``"package.module:func"``.
    When not set we simply return ``None`` so the task can respond with a
    helpful message instead of crashing the app on import.
    """

    global _cached_handler, _handler_attempted

    if _cached_handler is not None:
        return _cached_handler
    if _handler_attempted:
        return None

    _handler_attempted = True
    handler_path = os.getenv("RUNN_EXPORT_HANDLER", "").strip()
    if not handler_path:
        return None
    if ":" not in handler_path:
        raise RuntimeError(
            "RUNN_EXPORT_HANDLER debe tener el formato 'modulo:funcion'"
        )

    module_name, func_name = handler_path.split(":", 1)
    module = import_module(module_name)
    handler = getattr(module, func_name, None)
    if handler is None or not callable(handler):
        raise RuntimeError(
            "RUNN_EXPORT_HANDLER apunta a un objeto que no es callable"
        )
    _cached_handler = handler  # type: ignore[assignment]
    return _cached_handler


def export_runn_snapshot(*, window_days: int = 120) -> Dict[str, Any]:
    """Export a Runn snapshot using the configured handler.

    Parameters
    ----------
    window_days:
        Number of days to include in the export window.  The value is forwarded
        to the configured handler.

    Returns
    -------
    dict
        The handler response or a default payload when no handler has been
        configured.
    """

    try:
        handler = _resolve_handler()
    except Exception as error:  # pragma: no cover - defensive guard
        logging.getLogger(__name__).exception(
            "No se pudo resolver RUNN_EXPORT_HANDLER"
        )
        return {
            "ok": False,
            "reason": "Error al resolver RUNN_EXPORT_HANDLER",
            "details": str(error),
            "window_days": window_days,
        }

    if handler is None:
        return {
            "ok": False,
            "reason": "RUNN_EXPORT_HANDLER no está configurado",
            "window_days": window_days,
        }

    try:
        result = handler(window_days=window_days)
    except Exception as error:  # pragma: no cover - defensive guard
        logging.getLogger(__name__).exception(
            "Fallo la ejecución del handler RUNN_EXPORT_HANDLER"
        )
        return {
            "ok": False,
            "reason": "Error al ejecutar RUNN_EXPORT_HANDLER",
            "details": str(error),
            "window_days": window_days,
        }

    if isinstance(result, dict):
        return result
    return {"ok": True, "result": result, "window_days": window_days}
