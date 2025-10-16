from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

# Cloud Tasks client se importa bajo demanda para no romper local si falta la lib.
_tasks_v2 = None

def _require_tasks():
    global _tasks_v2
    if _tasks_v2 is not None:
        return _tasks_v2
    try:
        from google.cloud import tasks_v2  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "google-cloud-tasks no está instalado o no se pudo importar"
        ) from exc
    _tasks_v2 = tasks_v2
    return _tasks_v2


def enqueue_http_task(
    *,
    queue: str,
    relative_url: str,
    payload: Dict[str, Any],
    project: Optional[str] = None,
    location: Optional[str] = None,
    service_url: Optional[str] = None,
    service_account_email: Optional[str] = None,
    audience: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
) -> Dict[str, str]:
    """
    Crea una tarea HTTP en Cloud Tasks apuntando a ESTE servicio HTTP (Cloud Run).
    Usa OIDC para invocar autenticado el endpoint del worker.
    """

    tasks_v2 = _require_tasks()

    project = (project or os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT") or "").strip()
    if not project:
        raise RuntimeError("Falta GCP_PROJECT/GOOGLE_CLOUD_PROJECT")

    queue = (queue or "").strip()
    if not queue:
        raise RuntimeError("Falta el nombre de la cola (queue)")

    # IMPORTANTE: Cloud Tasks no soporta 'northamerica-south1'. Usa, por ejemplo, us-central1.
    location = (location or os.environ.get("TASKS_LOCATION") or "us-central1").strip()

    base_url = (service_url or os.environ.get("SERVICE_URL") or "").strip().rstrip("/")
    if not base_url:
        raise RuntimeError("Falta SERVICE_URL (la URL pública de tu servicio de Cloud Run)")

    target_url = f"{base_url}{relative_url}"
    http_headers = {"Content-Type": "application/json"}
    if headers:
        http_headers.update(headers)

    body_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    client = tasks_v2.CloudTasksClient()
    parent = client.queue_path(project, location, queue)

    oidc: Dict[str, str] = {}
    if service_account_email:
        oidc["service_account_email"] = service_account_email
        oidc["audience"] = (audience or base_url)

    task: Dict[str, Any] = {
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": target_url,
            "headers": http_headers,
            "body": body_bytes,
        }
    }
    if oidc:
        task["http_request"]["oidc_token"] = oidc

    created = client.create_task(request={"parent": parent, "task": task})
    return {"name": created.name, "url": target_url}
