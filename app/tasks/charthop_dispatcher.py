from __future__ import annotations

import os
from typing import Dict

from app.tasks.cloud import enqueue_http_task

_DEFAULT_QUEUE = "charthop-tasks"


def enqueue_charthop_task(kind: str, entity_id: str) -> Dict[str, str]:
    kind_value = (kind or "").strip().lower()
    entity_value = str(entity_id or "").strip()
    if not entity_value:
        raise ValueError("entity_id is required")

    queue = (
        os.environ.get("CHARTHOP_TASKS_QUEUE")
        or os.environ.get("TASKS_QUEUE")
        or _DEFAULT_QUEUE
    )
    project = os.environ.get("CHARTHOP_TASKS_PROJECT")
    location = os.environ.get("CHARTHOP_TASKS_LOCATION")
    service_url = (
        os.environ.get("CHARTHOP_SERVICE_URL")
        or os.environ.get("RUN_SERVICE_URL")
        or os.environ.get("SERVICE_URL")
    )
    service_account = os.environ.get("CHARTHOP_TASKS_SA_EMAIL") or os.environ.get("TASKS_SA_EMAIL")
    audience = os.environ.get("CHARTHOP_TASKS_AUDIENCE")

    payload = {"kind": kind_value, "entity_id": entity_value}
    return enqueue_http_task(
        queue=queue,
        relative_url="/tasks/worker",
        payload=payload,
        project=project,
        location=location,
        service_url=service_url,
        service_account_email=service_account,
        audience=audience,
    )
