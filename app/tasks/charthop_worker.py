from __future__ import annotations

import datetime as dt

from flask import Blueprint, jsonify, request

from app.services.ctc_calculator import (
    batch_calculate_and_update_ch_ctc,
    calculate_and_update_ch_ctc,
)
from app.services.runn_sync import (
    delete_runn_timeoff_event,
    sync_runn_compensation,
    sync_runn_compensation_event,
    sync_runn_onboarding_event,
    sync_runn_timeoff_event,
)

bp_charthop_tasks = Blueprint("charthop_tasks", __name__)


@bp_charthop_tasks.post("/tasks/worker")
def run_charthop_worker():
    payload = request.get_json(force=True, silent=True) or {}
    kind = (payload.get("kind") or "").strip().lower()
    entity_id = str(payload.get("entity_id") or "").strip()

    if not kind or not entity_id:
        return jsonify({"ok": False, "error": "missing kind/entity_id"}), 400

    if kind == "timeoff":
        result = sync_runn_timeoff_event(entity_id)
    elif kind == "timeoff_delete":
        result = delete_runn_timeoff_event(entity_id)
    elif kind == "person":
        result = sync_runn_onboarding_event(entity_id)
    elif kind == "compensation":
        result = sync_runn_compensation_event(entity_id)
    elif kind in {"compensation_batch", "compensation_sync_batch"}:
        reference = None
        if entity_id and entity_id != "batch_sync":
            try:
                reference = dt.datetime.strptime(entity_id, "%Y-%m-%d").date()
            except ValueError:
                reference = None
        result = sync_runn_compensation(reference)
    elif kind == "ctc_recalculate":
        result = calculate_and_update_ch_ctc(entity_id)
    elif kind == "ctc_recalculate_batch":
        result = batch_calculate_and_update_ch_ctc()
    else:
        result = {"status": "ignored", "reason": "unknown kind", "kind": kind, "entity_id": entity_id}

    return jsonify({"ok": True, "kind": kind, "entity_id": entity_id, "result": result}), 200
