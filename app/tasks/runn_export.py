# app/tasks/runn_export.py
from flask import Blueprint, request, jsonify
from app.services.runn_bq_export import export_runn_snapshot

bp_tasks = Blueprint("tasks", __name__, url_prefix="/tasks")

@bp_tasks.post("/export-runn")
def run_export_runn():
    window_days = max(int(request.args.get("window_days", "60")), 0)
    result = export_runn_snapshot(window_days=window_days)
    return jsonify(result), 200
