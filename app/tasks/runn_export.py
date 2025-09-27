from __future__ import annotations

import logging
import os
from flask import jsonify, request
from flask.typing import ResponseReturnValue

from app.tasks.ca_export import bp_tasks

# Permite inyectar el handler por env; default apunta a run_full_sync
# NO cambies el nombre de la variable de entorno.
try:
    handler_path = os.getenv(
        "RUNN_EXPORT_HANDLER",
        "app.handlers.runn_full_sync:run_full_sync",
    )
    mod_name, func_name = handler_path.split(":")
    mod = __import__(mod_name, fromlist=[func_name])
    export_handler = getattr(mod, func_name)
except Exception as import_error:
    logging.getLogger(__name__).error(
        "No se pudo importar RUNN_EXPORT_HANDLER: %s", import_error
    )

    def export_handler(*args, **kwargs):
        return {
            "ok": False,
            "reason": "No se pudo importar RUNN_EXPORT_HANDLER",
            "details": str(import_error),
        }


@bp_tasks.post("/tasks/export-runn")
def run_export_runn() -> ResponseReturnValue:
    """
    Invocado por Cloud Tasks o cron.
    Query params:
      - window_days: int (default=env WINDOW_DAYS o 120)
      - targets: coma-separado de recursos (opcional)
    """
    window_days = int(request.args.get("window_days", os.getenv("WINDOW_DAYS", "120")))
    targets = request.args.get("targets")
    target_list = [t.strip() for t in targets.split(",")] if targets else None

    result = export_handler(window_days=window_days, targets=target_list)
    return jsonify({"ok": True, "result": result}), 200
