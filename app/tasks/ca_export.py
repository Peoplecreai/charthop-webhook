from __future__ import annotations

import os
import json
import time
from typing import Optional

from flask import Blueprint, jsonify, request
from google.cloud import tasks_v2

from app.services.culture_amp import export_culture_amp_snapshot

bp_tasks = Blueprint("tasks", __name__)

# Config leída de entorno
GCP_PROJECT = os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT")
TASKS_LOCATION = os.environ.get("TASKS_LOCATION", "northamerica-south1")
TASKS_QUEUE = os.environ.get("TASKS_QUEUE", "export-queue")
RUN_SERVICE_URL = os.environ.get("RUN_SERVICE_URL")  # p.ej. https://<servicio>.northamerica-south1.run.app
TASKS_SA_EMAIL = os.environ.get("TASKS_SA_EMAIL")    # p.ej. tasks-runner@integration-hub-468417.iam.gserviceaccount.com


def enqueue_export_task(payload: Optional[dict] = None) -> dict:
    """
    Encola una tarea HTTP hacia /tasks/export-culture-amp con OIDC.
    Retorna datos del task creado.
    """
    if not (GCP_PROJECT and TASKS_LOCATION and TASKS_QUEUE and RUN_SERVICE_URL and TASKS_SA_EMAIL):
        raise RuntimeError(
            "Faltan variables para Cloud Tasks: GCP_PROJECT/GOOGLE_CLOUD_PROJECT, "
            "TASKS_LOCATION, TASKS_QUEUE, RUN_SERVICE_URL, TASKS_SA_EMAIL"
        )

    client = tasks_v2.CloudTasksClient()
    parent = client.queue_path(GCP_PROJECT, TASKS_LOCATION, TASKS_QUEUE)

    url = f"{RUN_SERVICE_URL.rstrip('/')}/tasks/export-culture-amp"
    body_bytes = json.dumps(payload or {}).encode("utf-8")

    task = {
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": url,
            "headers": {"Content-Type": "application/json"},
            "body": body_bytes,
            "oidc_token": {
                "service_account_email": TASKS_SA_EMAIL,
                # La audiencia debe ser el ORIGEN del servicio (sin path)
                "audience": RUN_SERVICE_URL,
            },
        }
    }

    created = client.create_task(request={"parent": parent, "task": task})
    return {"name": created.name, "url": url}


@bp_tasks.post("/tasks/export-culture-amp")
def run_export_task():
    """
    Ejecuta la exportación completa (invocado por Cloud Tasks con OIDC).
    """
    t0 = time.time()
    try:
        result = export_culture_amp_snapshot()
        elapsed_ms = int((time.time() - t0) * 1000)
        return jsonify({"ok": True, "elapsed_ms": elapsed_ms, "result": result}), 200
    except Exception as exc:  # pragma: no cover
        return jsonify({"ok": False, "error": str(exc)}), 500
