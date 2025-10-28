from __future__ import annotations

from flask import Blueprint, jsonify, request

from app.services.runn_sync import (
    delete_runn_timeoff_event,
    sync_runn_onboarding_event,
    sync_runn_timeoff_event,
)

bp_charthop_tasks = Blueprint("charthop_tasks", __name__)


@bp_charthop_tasks.post("/tasks/worker")
def run_charthop_worker():
    """
    Procesa eventos de ChartHop encolados por Cloud Tasks.

    Formatos soportados:
    1. Webhook estilo ChartHop:
       {
         "kind": "time_off.created",
         "entity_id": "<timeoff_id>",
         "payload": {...}
       }

    2. Formato simplificado:
       {
         "kind": "timeoff",
         "entity_id": "<timeoff_id>"
       }
    """
    payload = request.get_json(force=True, silent=True) or {}
    kind = (payload.get("kind") or "").strip().lower()
    entity_id = str(payload.get("entity_id") or "").strip()

    if not kind:
        return jsonify({"ok": False, "error": "missing kind"}), 400

    # Normalizar el kind: "time_off.created" -> "timeoff"
    # "time_off.updated" -> "timeoff"
    # "time_off.deleted" -> "timeoff_delete"
    # "person.created" -> "person"
    # "person.updated" -> "person"
    if "time_off" in kind or "timeoff" in kind:
        if "delet" in kind or "remov" in kind:
            kind = "timeoff_delete"
        else:
            kind = "timeoff"
    elif "person" in kind:
        kind = "person"

    if not entity_id:
        return jsonify({"ok": False, "error": "missing entity_id"}), 400

    # Procesar según el tipo
    if kind == "timeoff":
        result = sync_runn_timeoff_event(entity_id)
    elif kind == "timeoff_delete":
        result = delete_runn_timeoff_event(entity_id)
    elif kind == "person":
        result = sync_runn_onboarding_event(entity_id)
    else:
        result = {"status": "ignored", "reason": "unknown kind", "kind": kind, "entity_id": entity_id}

    return jsonify({"ok": True, "kind": kind, "entity_id": entity_id, "result": result}), 200
