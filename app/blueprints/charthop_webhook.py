from flask import Blueprint, request

from app.services.job_sync import sync_job_create, sync_job_update
from app.tasks.charthop_dispatcher import enqueue_charthop_task

bp_ch = Blueprint("charthop_webhook", __name__)

@bp_ch.route("/webhooks/charthop", methods=["GET", "POST"])
def ch_webhook():
    if request.method == "GET":
        return "ChartHop webhook up", 200

    evt = request.get_json(force=True, silent=True) or {}
    evtype_raw = (evt.get("type") or evt.get("eventType") or evt.get("event_type") or "").lower()
    evtype = evtype_raw.replace("-", ".")
    entity = (evt.get("entityType") or evt.get("entitytype") or evt.get("entity_type") or "").lower()
    entity_id = str(evt.get("entityId") or evt.get("entityid") or evt.get("entity_id") or "")

    type_parts = [part for part in evtype.split(".") if part]
    type_entity = type_parts[0] if len(type_parts) >= 2 else ""
    action = type_parts[-1] if type_parts else evtype
    if not entity and type_entity:
        entity = type_entity

    print(f"CH evt: type={evtype_raw} entity={entity} entity_id={entity_id} action={action}")
    is_job = entity in ("job", "jobs")
    is_timeoff = entity in ("timeoff", "time off", "time-off") or type_entity == "timeoff"
    is_person = entity in ("person", "people") or type_entity == "person"
    is_create = action in ("create", "created")
    is_update = action in ("update", "updated", "change", "changed")
    is_delete = action in ("delete", "deleted", "remove", "removed")

    if is_timeoff and entity_id:
        try:
            # Determinar el tipo de tarea según la acción
            if is_delete:
                task = enqueue_charthop_task("timeoff_delete", entity_id)
                print(f"Queued ChartHop timeoff delete task: {task}")
            else:
                # Create o Update se manejan con la misma función (auto-detecta)
                task = enqueue_charthop_task("timeoff", entity_id)
                print(f"Queued ChartHop timeoff sync task: {task}")
        except Exception as exc:  # pragma: no cover - defensive logging
            print(f"Failed to enqueue timeoff task: {exc}")
            return "", 500
        return "", 200

    if is_person and entity_id and (is_create or is_update):
        try:
            task = enqueue_charthop_task("person", entity_id)
            print(f"Queued ChartHop person task: {task}")
        except Exception as exc:  # pragma: no cover - defensive logging
            print(f"Failed to enqueue person task: {exc}")
            return "", 500
        return "", 200

    if not is_job:
        return "", 200

    if is_create:
        if not entity_id:
            print("CH job create: missing entity_id"); return "", 200
        sync_job_create(entity_id)

    if is_update and entity_id:
        sync_job_update(entity_id)
    return "", 200

