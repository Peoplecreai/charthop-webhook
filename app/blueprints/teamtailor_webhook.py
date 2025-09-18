from flask import Blueprint, request
from app.utils.config import tt_verify_signature
from app.clients.teamtailor import tt_fetch_application
from app.services.hire import process_hired_application

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
        result = process_hired_application(rid, body)
        print("TT hire result:", result)
        return "", 200

    except Exception as e:
        print("tt_webhook error:", repr(e))
        return "", 200

