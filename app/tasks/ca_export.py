from __future__ import annotations

import os
import time
from typing import Optional

from flask import Blueprint, jsonify

from app.services.culture_amp import export_culture_amp_snapshot
from app.tasks.cloud import enqueue_http_task

bp_tasks = Blueprint("tasks", __name__)


def _load_cfg() -> dict:
    project = os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT")
    location = os.environ.get("TASKS_LOCATION", "us-central1")
    queue = os.environ.get("TASKS_QUEUE", "export-queue")
    run_service_url = (os.environ.get("RUN_SERVICE_URL") or "").strip()
    sa_email = (os.environ.get("TASKS_SA_EMAIL") or "").strip()

    missing = []
    if not project:
        missing.append("GCP_PROJECT/GOOGLE_CLOUD_PROJECT")
    if not run_service_url:
        missing.append("RUN_SERVICE_URL")
    if not sa_email:
        missing.append("TASKS_SA_EMAIL")
    if missing:
        raise RuntimeError("Faltan variables para Cloud Tasks: " + ", ".join(missing))

    return {
        "project": project,
        "location": location,
        "queue": queue,
        "run_service_url": run_service_url,
        "service_account_email": sa_email,
    }


def enqueue_export_task(payload: Optional[dict] = None) -> dict:
    cfg = _load_cfg()
    return enqueue_http_task(
        queue=cfg["queue"],
        relative_url="/tasks/export-culture-amp",
        payload=payload,
        project=cfg["project"],
        location=cfg["location"],
        service_url=cfg["run_service_url"],
        service_account_email=cfg["service_account_email"],
        audience=cfg["run_service_url"],
    )


@bp_tasks.post("/tasks/export-culture-amp")
def run_export_task():
    t0 = time.time()
    try:
        result = export_culture_amp_snapshot()
        elapsed_ms = int((time.time() - t0) * 1000)
        return jsonify({"ok": True, "elapsed_ms": elapsed_ms, "result": result}), 200
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500
