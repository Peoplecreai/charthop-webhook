# app/tasks/runn_export.py
from flask import jsonify, request
from flask.typing import ResponseReturnValue

import logging

try:
    from app.services.runn_bq_export import export_runn_snapshot
except ModuleNotFoundError as import_error:  # pragma: no cover - defensive guard
    logging.getLogger(__name__).error(
        "No se pudo importar app.services.runn_bq_export: %s", import_error
    )

    def export_runn_snapshot(*, window_days: int = 120):
        """Fallback cuando el módulo opcional no está disponible."""

        return {
            "ok": False,
            "reason": "El módulo app.services.runn_bq_export no está disponible",
            "details": str(import_error),
            "window_days": window_days,
        }
from app.tasks.ca_export import bp_tasks

@bp_tasks.post("/tasks/export-runn")
def run_export_runn() -> ResponseReturnValue:
    window_days = int(request.args.get("window_days", "120"))
    result = export_runn_snapshot(window_days=window_days)
    return jsonify({"ok": True, "result": result}), 200
