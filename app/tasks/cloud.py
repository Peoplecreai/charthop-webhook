from __future__ import annotations

import json
import os
from importlib import import_module
from typing import Any, Dict, Optional

_tasks_module = None


def _require_tasks_module():
    global _tasks_module
    if _tasks_module is not None:
        return _tasks_module
    try:
        module = import_module("google.cloud.tasks_v2")
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "google-cloud-tasks no está instalado. Agrega 'google-cloud-tasks' a tus dependencias."
        ) from exc
    _tasks_module = module
    return module


def enqueue_http_task(
    *,
    queue: str,
    relative_url: str,
    payload: Optional[Dict[str, Any]] = None,
    project: Optional[str] = None,
    location: Optional[str] = None,
    service_url: Optional[str] = None,
    service_account_email: Optional[str] = None,
    audience: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
) -> Dict[str, str]:
    """Crea una tarea HTTP en Cloud Tasks apuntando a un servicio HTTP/Cloud Run."""

    tasks_v2 = _require_tasks_module()

    project = (project or os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT") or "").strip()
    if not project:
        raise RuntimeError("Falta GCP_PROJECT/GOOGLE_CLOUD_PROJECT para Cloud Tasks")

    queue = (queue or "").strip()
    if not queue:
        raise RuntimeError("Falta nombre de la cola de Cloud Tasks")

    location = (location or os.environ.get("TASKS_LOCATION") or "us-central1").strip()
    service_url = (
        service_url
        or os.environ.get("RUN_SERVICE_URL")
        or os.environ.get("SERVICE_URL")
        or ""
    ).strip()
    if not service_url:
        raise RuntimeError("Falta RUN_SERVICE_URL o SERVICE_URL para Cloud Tasks")

    service_account_email = (service_account_email or os.environ.get("TASKS_SA_EMAIL") or "").strip()
    if not service_account_email:
        raise RuntimeError("Falta TASKS_SA_EMAIL para Cloud Tasks")

    audience = (audience or service_url).strip()

    target_url = f"{service_url.rstrip('/')}/{relative_url.lstrip('/')}"
    http_headers = {"Content-Type": "application/json"}
    if headers:
        http_headers.update(headers)

    body_bytes = json.dumps(payload or {}).encode("utf-8")

    client = tasks_v2.CloudTasksClient()
    parent = client.queue_path(project, location, queue)
    task = {
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": target_url,
            "headers": http_headers,
            "body": body_bytes,
            "oidc_token": {
                "service_account_email": service_account_email,
                "audience": audience,
            },
        }
    }

    created = client.create_task(request={"parent": parent, "task": task})
    return {"name": created.name, "url": target_url}
