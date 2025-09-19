from __future__ import annotations

import datetime as dt
import time
import traceback
from typing import Optional

from flask import Blueprint, jsonify, request, current_app

from app.tasks.ca_export import enqueue_export_task
from app.services.runn_sync import sync_runn_onboarding, sync_runn_timeoff

bp_cron = Blueprint("cron", __name__)


def _parse_yyyy_mm_dd(s: Optional[str]) -> Optional[dt.date]:
    if not s:
        return None
    try:
        return dt.datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return None


def _json_ok(payload: dict, status_code: int = 200):
    resp = jsonify(payload)
    resp.status_code = status_code
    resp.headers["Cache-Control"] = "no-store"
    return resp


@bp_cron.route("/cron/nightly", methods=["GET", "POST"])
def nightly():
    """
    Cloud Scheduler: ENCOLA y responde inmediato.
    Cualquier error (config, IAM, queue inexistente, lib ausente) responde JSON 503,
    no 500 HTML.
    """
    t0 = time.time()
    try:
        task = enqueue_export_task()
        elapsed_ms = int((time.time() - t0) * 1000)
        return _json_ok({"status": "queued", "elapsed_ms": elapsed_ms, "task": task}, 200)
    except Exception as exc:  # capturamos TODO
        elapsed_ms = int((time.time() - t0) * 1000)
        current_app.logger.error("Nightly enqueue failed: %s\n%s", exc, traceback.format_exc())
        # 503 para que el Scheduler lo trate como reintentable
        return _json_ok(
            {"status": "error", "elapsed_ms": elapsed_ms, "message": str(exc)},
            503,
        )


@bp_cron.route("/cron/runn/onboarding", methods=["GET", "POST"])
def runn_onboarding():
    t0 = time.time()
    ref = request.args.get("date")
    reference = _parse_yyyy_mm_dd(ref) if ref else None
    try:
        result = sync_runn_onboarding(reference)
        elapsed_ms = int((time.time() - t0) * 1000)
        return _json_ok(
            {"status": "ok", "elapsed_ms": elapsed_ms, "reference_date": reference.isoformat() if reference else None, "result": result},
            200,
        )
    except Exception as exc:  # pragma: no cover - logging
        current_app.logger.error("runn_onboarding error: %s\n%s", exc, traceback.format_exc())
        return _json_ok(
            {"status": "error", "message": str(exc), "reference_date": reference.isoformat() if reference else None},
            500,
        )


@bp_cron.route("/cron/runn/timeoff", methods=["GET", "POST"])
def runn_timeoff():
    t0 = time.time()
    ref = request.args.get("date")
    reference = _parse_yyyy_mm_dd(ref) if ref else None
    try:
        result = sync_runn_timeoff(reference)
        elapsed_ms = int((time.time() - t0) * 1000)
        return _json_ok(
            {"status": "ok", "elapsed_ms": elapsed_ms, "reference_date": reference.isoformat() if reference else None, "result": result},
            200,
        )
    except Exception as exc:  # pragma: no cover - logging
        current_app.logger.error("runn_timeoff error: %s\n%s", exc, traceback.format_exc())
        return _json_ok(
            {"status": "error", "message": str(exc), "reference_date": reference.isoformat() if reference else None},
            500,
        )
