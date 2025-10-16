from flask import Blueprint, request
from app.services.job_sync import sync_job_create, sync_job_update
from app.services.runn_sync import sync_runn_timeoff_event

bp_ch = Blueprint("charthop_webhook", __name__)

@bp_ch.route("/webhooks/charthop", methods=["GET", "POST"])
def ch_webhook():
    if request.method == "GET":
        return "ChartHop webhook up", 200

    evt = request.get_json(force=True, silent=True) or {}
    evtype = (evt.get("type") or evt.get("eventType") or evt.get("event_type") or "").lower()
    entity = (evt.get("entityType") or evt.get("entitytype") or evt.get("entity_type") or "").lower()
    entity_id = str(evt.get("entityId") or evt.get("entityid") or evt.get("entity_id") or "")

    print(f"CH evt: type={evtype} entity={entity} entity_id={entity_id}")
    is_job = entity in ("job", "jobs")
    is_timeoff = entity in ("timeoff", "time off", "time-off")
    is_create = evtype in ("job.create", "job_create", "create")
    is_update = evtype in ("job.update", "job_update", "update", "change")

    if is_timeoff and entity_id:
        result = sync_runn_timeoff_event(entity_id)
        print(f"CH timeoff sync result: {result}")
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

