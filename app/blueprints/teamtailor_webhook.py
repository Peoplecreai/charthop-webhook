from flask import Blueprint, request
from app.utils.config import tt_verify_signature
from app.clients.teamtailor import tt_fetch_application, tt_get_offer_start_date_for_application
from app.clients.charthop import ch_import_people_csv

bp_tt = Blueprint("teamtailor_webhook", __name__)

@bp_tt.route("/webhooks/teamtailor", methods=["POST"])
def tt_webhook():
    try:
        payload = request.get_json(force=True, silent=True) or {}
        rid = str(payload.get("resource_id") or payload.get("id") or "")
        sig = request.headers.get("Teamtailor-Signature", "")

        if not tt_verify_signature(rid, sig):
            print(f"TT sig fail rid={rid}")
            return "", 200
        print(f"TT sig ok rid={rid}")

        if not rid:
            print("TT webhook: missing resource_id"); return "", 200

        resp = tt_fetch_application(rid)
        print("TT fetch status:", resp.status_code)
        if not resp.ok:
            return "", 200

        body = resp.json() or {}
        data = body.get("data") or {}
        attributes = data.get("attributes") or {}
        status = (attributes.get("status") or attributes.get("state") or "").lower()
        hired_at = attributes.get("hired-at") or attributes.get("hired_at")

        if status != "hired" and not hired_at:
            print("TT webhook: application not hired, skipping"); return "", 200

        included = body.get("included") or []
        cand = next((i for i in included if i.get("type") == "candidates"), {}) or {}
        job  = next((i for i in included if i.get("type") == "jobs"), {}) or {}

        cand_attr = cand.get("attributes") or {}
        job_attr  = job.get("attributes") or {}

        first = cand_attr.get("first-name") or cand_attr.get("first_name") or ""
        last  = cand_attr.get("last-name") or cand_attr.get("last_name") or ""
        personal_email = cand_attr.get("email") or ""
        title = job_attr.get("title") or ""

        # Start date desde Offer
        start_date = tt_get_offer_start_date_for_application(rid) or (attributes.get("start-date") or attributes.get("start_date") or (hired_at or ""))[:10]

        rows = [{
            "contact personalemail": personal_email,
            "first name": first,
            "last name": last,
            "title": title,
            "start date": start_date or ""
        }]
        ch_import_people_csv(rows)
        print("CH import ok")
        return "", 200

    except Exception as e:
        print("tt_webhook error:", repr(e))
        return "", 200

