from flask import Blueprint, request
from app.utils.config import CH_CF_JOB_TT_ID_LABEL
from app.clients.charthop import ch_find_job, ch_upsert_job_field
from app.clients.teamtailor import tt_create_job_from_ch, tt_upsert_job_custom_field

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
    is_create = evtype in ("job.create", "job_create", "create")
    is_update = evtype in ("job.update", "job_update", "update", "change")

    if not is_job:
        return "", 200

    if is_create:
        if not entity_id:
            print("CH job create: missing entity_id"); return "", 200
        job = ch_find_job(entity_id)
        if not job:
            print(f"CH job create: job {entity_id} not found"); return "", 200

        title = job.get("title") or "Untitled"
        r = tt_create_job_from_ch(title)
        print("TT job create status:", r.status_code, str(r.text)[:200])
        if not r.ok:
            return "", 200
        tt_job_id = ((r.json() or {}).get("data") or {}).get("id")
        if tt_job_id:
            try:
                tt_upsert_job_custom_field(tt_job_id, entity_id)
            except Exception as e:
                print("TT set charthop_job_id error:", e)
            try:
                ch_upsert_job_field(entity_id, CH_CF_JOB_TT_ID_LABEL, tt_job_id)
            except Exception as e:
                print("CH set teamtailorJobid error:", e)

    if is_update:
        print(f"CH job update received for {entity_id} (PATCH en TT opcional)")
    return "", 200

