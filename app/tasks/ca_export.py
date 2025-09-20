from __future__ import annotations

import json
import os
import time
from typing import Optional

from flask import Blueprint, jsonify

from google.protobuf import duration_pb2
from google.api_core import exceptions as gexc
import datetime as dt

import socket, logging, time as _t, os

# Dependencia opcional: si no está instalada, fallamos con un error claro
try:  # pragma: no cover - dependencia opcional en runtime
    from google.cloud import tasks_v2  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - logging
    tasks_v2 = None  # type: ignore[assignment]

from app.services.culture_amp import export_culture_amp_snapshot

bp_tasks = Blueprint("tasks", __name__)


def _require_tasks_module():
    if tasks_v2 is None:
        raise RuntimeError(
            "google-cloud-tasks no está instalado. Agrega 'google-cloud-tasks' a tus dependencias "
            "o desactiva el cron de Culture Amp."
        )
    return tasks_v2


def _load_config() -> dict:
    project = os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT")
    location = os.environ.get("TASKS_LOCATION", "northamerica-south1")
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
    tasks_module = _require_tasks_module()
    cfg = _load_config()

    client = tasks_module.CloudTasksClient()
    parent = client.queue_path(cfg["project"], cfg["location"], cfg["queue"])

    url = f"{cfg['run_service_url'].rstrip('/')}/tasks/export-culture-amp"
    body_bytes = json.dumps(payload or {}).encode("utf-8")
    deadline = duration_pb2.Duration(seconds=900)

    # task_id fijo por día (cambia si quieres por hora/minuto)
    task_id = f"export-ca-{dt.date.today().isoformat()}"
    name = client.task_path(cfg["project"], cfg["location"], cfg["queue"], task_id)

    task = {
        "name": name,  # ← evita duplicados
        "http_request": {
            "http_method": tasks_module.HttpMethod.POST,
            "url": url,
            "headers": {"Content-Type": "application/json"},
            "body": body_bytes,
            "oidc_token": {
                "service_account_email": cfg["service_account_email"],
                "audience": cfg["run_service_url"],
            },
        },
        "dispatch_deadline": deadline,
    }

    try:
        created = client.create_task(request={"parent": parent, "task": task})
        return {"name": created.name, "url": url}
    except gexc.AlreadyExists:
        # Ya hay una tarea para hoy: lo consideramos OK/idempotente
        return {"name": name, "url": url, "status": "already_enqueued"}


def _probe_sftp(host: str, port: int = 22, deadline: float = 10.0) -> bool:
    if not host:
        logging.error("CA_SFTP_HOST vacío")
        return False
    t0 = _t.time()
    try:
        with socket.create_connection((host.strip().rstrip("."), port), timeout=deadline):
            logging.warning("SFTP TCP connect OK to %s:%d in %.2fs", host, port, _t.time()-t0)
            return True
    except Exception as e:
        logging.error("SFTP TCP connect FAIL to %s:%d: %r (%.2fs)", host, port, e, _t.time()-t0)
        return False

@bp_tasks.post("/tasks/export-culture-amp")
def run_export_task():
    t0 = time.time()
    host = os.environ.get("CA_SFTP_HOST", "")
    if not _probe_sftp(host):
        # Responde rápido: Cloud Tasks reintenta sin consumir 10 minutos
        return jsonify({"ok": False, "error": "sftp_tcp_connect_fail", "host": host}), 502
    try:
        result = export_culture_amp_snapshot()
        elapsed_ms = int((time.time() - t0) * 1000)
        return jsonify({"ok": True, "elapsed_ms": elapsed_ms, "result": result}), 200
    except Exception as exc:  # pragma: no cover
        return jsonify({"ok": False, "error": str(exc)}), 500
