# app/tasks/runn_export.py
from flask import request, jsonify

from app.services.runn_bq_export import export_runn_snapshot
from app.tasks.ca_export import bp_tasks

@bp_tasks.post("/tasks/export-runn")
def run_export_runn():
    window_days = int(request.args.get("window_days", "120"))
    result = export_runn_snapshot(window_days=window_days)
    return jsonify({"ok": True, "result": result}), 200
