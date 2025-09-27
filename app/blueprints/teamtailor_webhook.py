from __future__ import annotations

import logging

from flask import Blueprint, request
from flask.typing import ResponseReturnValue

from app.clients.teamtailor import tt_fetch_application
from app.services.hire import process_hired_application
from app.utils.config import tt_verify_signature

logger = logging.getLogger(__name__)

bp_tt = Blueprint("teamtailor_webhook", __name__)

@bp_tt.route("/webhooks/teamtailor", methods=["POST"])
def tt_webhook() -> ResponseReturnValue:
    try:
        payload = request.get_json(force=True, silent=True) or {}
        rid = str(payload.get("resource_id") or payload.get("id") or "")
        sig = request.headers.get("Teamtailor-Signature", "")

        if not tt_verify_signature(rid, sig):
            logger.warning("Teamtailor signature validation failed for resource_id=%s", rid)
            return "", 200
        logger.info("Teamtailor signature validated for resource_id=%s", rid)

        if not rid:
            logger.warning("Teamtailor webhook missing resource_id")
            return "", 200

        resp = tt_fetch_application(rid)
        logger.info("Fetched Teamtailor application %s with status %s", rid, resp.status_code)
        if not resp.ok:
            return "", 200

        body = resp.json() or {}
        result = process_hired_application(rid, body)
        logger.info("Processed Teamtailor hire for resource_id=%s result=%s", rid, result)
        return "", 200

    except Exception as e:
        logger.exception("Unhandled error in Teamtailor webhook")
        return "", 200

