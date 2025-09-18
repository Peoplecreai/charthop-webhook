from __future__ import annotations

import datetime as dt

from flask import Blueprint, jsonify, request

from app.services.culture_amp import export_culture_amp_snapshot
from app.services.runn_sync import sync_runn_onboarding, sync_runn_timeoff

bp_cron = Blueprint("cron", __name__)


@bp_cron.route("/cron/nightly", methods=["GET"])
def nightly():
    try:
        result = export_culture_amp_snapshot()
        return jsonify({"status": "ok", "result": result})
    except Exception as exc:  # pragma: no cover - logging
        print("nightly export error:", repr(exc))
        return jsonify({"status": "error", "message": str(exc)}), 500


@bp_cron.route("/cron/runn/onboarding", methods=["GET"])
def runn_onboarding():
    ref = request.args.get("date")
    reference = None
    if ref:
        try:
            reference = dt.datetime.strptime(ref, "%Y-%m-%d").date()
        except ValueError:
            reference = None
    result = sync_runn_onboarding(reference)
    return jsonify(result)


@bp_cron.route("/cron/runn/timeoff", methods=["GET"])
def runn_timeoff():
    ref = request.args.get("date")
    reference = None
    if ref:
        try:
            reference = dt.datetime.strptime(ref, "%Y-%m-%d").date()
        except ValueError:
            reference = None
    result = sync_runn_timeoff(reference)
    return jsonify(result)
