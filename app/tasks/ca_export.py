"""Cloud Tasks integration for asynchronously running Culture Amp exports."""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from importlib import import_module
from typing import Optional, Protocol

from flask import Blueprint, jsonify
from flask.typing import ResponseReturnValue

from app.services.culture_amp import export_culture_amp_snapshot

bp_tasks = Blueprint("tasks", __name__)

_tasks_module = None

logger = logging.getLogger(__name__)


class _TasksModule(Protocol):
    class CloudTasksClient:  # pragma: no cover - protocol definition
        def queue_path(self, project: str, location: str, queue: str) -> str: ...

        def create_task(self, request: dict) -> object: ...

    class HttpMethod:  # pragma: no cover - protocol definition
        POST: str


@dataclass(frozen=True)
class CloudTasksConfig:
    project: str
    location: str
    queue: str
    run_service_url: str
    service_account_email: str

    @classmethod
    def from_env(cls) -> "CloudTasksConfig":
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

        return cls(
            project=project,
            location=location,
            queue=queue,
            run_service_url=run_service_url,
            service_account_email=sa_email,
        )


def _require_tasks_module() -> _TasksModule:
    global _tasks_module
    if _tasks_module is not None:
        return _tasks_module
    try:
        module = import_module("google.cloud.tasks_v2")
    except ModuleNotFoundError as exc:  # pragma: no cover - defensive guard
        raise RuntimeError(
            "google-cloud-tasks no está instalado. Agrega 'google-cloud-tasks' a tus dependencias."
        ) from exc
    _tasks_module = module
    return module


def enqueue_export_task(payload: Optional[dict] = None) -> dict:
    tasks_v2 = _require_tasks_module()
    cfg = CloudTasksConfig.from_env()
    client = tasks_v2.CloudTasksClient()
    parent = client.queue_path(cfg.project, cfg.location, cfg.queue)

    url = f"{cfg.run_service_url.rstrip('/')}/tasks/export-culture-amp"
    body_bytes = json.dumps(payload or {}).encode("utf-8")

    task = {
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": url,
            "headers": {"Content-Type": "application/json"},
            "body": body_bytes,
            "oidc_token": {
                "service_account_email": cfg.service_account_email,
                "audience": cfg.run_service_url,
            },
        }
    }

    created = client.create_task(request={"parent": parent, "task": task})
    task_name = getattr(created, "name", "")
    logger.info("Scheduled Culture Amp export task name=%s", task_name)
    return {"name": task_name, "url": url}


@bp_tasks.post("/tasks/export-culture-amp")
def run_export_task() -> ResponseReturnValue:
    t0 = time.time()
    try:
        result = export_culture_amp_snapshot()
        elapsed_ms = int((time.time() - t0) * 1000)
        return jsonify({"ok": True, "elapsed_ms": elapsed_ms, "result": result}), 200
    except Exception as exc:  # pragma: no cover - defensive guard
        logger.exception("Culture Amp export failed")
        return jsonify({"ok": False, "error": str(exc)}), 500

