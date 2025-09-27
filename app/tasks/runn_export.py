# app/tasks/runn_export.py
from flask import jsonify, request
import logging, os

from app.tasks.ca_export import bp_tasks

def _import_handler():
    handler_path = os.getenv("RUNN_EXPORT_HANDLER", "app.handlers.runn_full_sync:run_full_sync")
    try:
        mod_name, func_name = handler_path.split(":")
        mod = __import__(mod_name, fromlist=[func_name])
        return getattr(mod, func_name)
    except Exception as e:
        logging.getLogger(__name__).exception("No se pudo importar RUNN_EXPORT_HANDLER")
        return lambda **kwargs: {"ok": False, "error": str(e)}

@bp_tasks.post("/tasks/export-runn")
def run_export_runn():
    handler = _import_handler()
    window_days = int(request.args.get("window_days", os.getenv("WINDOW_DAYS", "120")))
    targets = request.args.get("targets")
    target_list = [t.strip() for t in targets.split(",")] if targets else None
    result = handler(window_days=window_days, targets=target_list)
    return jsonify({"ok": True, "result": result}), 200
