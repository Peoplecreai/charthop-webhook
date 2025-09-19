from __future__ import annotations

import datetime as dt
import time
from typing import Optional

from flask import Blueprint, jsonify, request, current_app

from app.tasks.ca_export import enqueue_export_task, CloudTasksNotConfigured
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
    Cloud Scheduler: ENCOLA y responde inmediato para evitar 504.
    El trabajo real lo ejecuta /tasks/export-culture-amp vía Cloud Tasks.
    """
    t0 = time.time()
    try:
        task = enqueue_export_task()
        elapsed_ms = int((time.time() - t0) * 1000)
        return _json_ok({"status": "queued", "elapsed_ms": elapsed_ms, "task": task}, 200)
    except CloudTasksNotConfigured as exc:
        elapsed_ms = int((time.time() - t0) * 1000)
        current_app.logger.warning("Nightly export skipped: %s", exc)
        return _json_ok(
            {
                "status": "skipped",
                "elapsed_ms": elapsed_ms,
                "message": str(exc),
            },
            200,
        )
    except RuntimeError as exc:
        elapsed_ms = int((time.time() - t0) * 1000)
        current_app.logger.error("Nightly export failed: %s", exc)
        return _json_ok(
            {
                "status": "error",
                "elapsed_ms": elapsed_ms,
                "message": str(exc),
            },
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
            {
                "status": "ok",
                "elapsed_ms": elapsed_ms,
                "reference_date": reference.isoformat() if reference else None,
                "result": result,
            },
            200,
        )
    except Exception as exc:  # pragma: no cover - logging
        return _json_ok(
            {
                "status": "error",
                "message": str(exc),
                "reference_date": reference.isoformat() if reference else None,
            },
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
            {
                "status": "ok",
                "elapsed_ms": elapsed_ms,
                "reference_date": reference.isoformat() if reference else None,
                "result": result,
            },
            200,
        )
    except Exception as exc:  # pragma: no cover - logging
        return _json_ok(
            {
                "status": "error",
                "message": str(exc),
                "reference_date": reference.isoformat() if reference else None,
            },
            500,
        )
