from __future__ import annotations

import datetime as dt
import time
from typing import Optional

from flask import Blueprint, jsonify, request

from app.services.culture_amp import export_culture_amp_snapshot
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
    # Evita caching intermedio en proxies
    resp.headers["Cache-Control"] = "no-store"
    return resp


@bp_cron.route("/cron/nightly", methods=["GET", "POST"])
def nightly():
    """
    Cloud Scheduler endpoint.
    Debe responder 200 incluso cuando no haya filas que exportar.
    """
    t0 = time.time()
    try:
        result = export_culture_amp_snapshot()
        elapsed_ms = int((time.time() - t0) * 1000)
        status = "ok" if not result.get("skipped") else "skipped"
        return _json_ok(
            {
                "status": status,
                "elapsed_ms": elapsed_ms,
                "result": result,
            },
            200,
        )
    except Exception as exc:  # pragma: no cover - logging
        # Devolvemos 500 solo en errores reales (bugs/credenciales/red)
        print("nightly export error:", repr(exc))
        return _json_ok(
            {
                "status": "error",
                "message": str(exc),
            },
            500,
        )


@bp_cron.route("/cron/runn/onboarding", methods=["GET", "POST"])
def runn_onboarding():
    """
    Sincroniza onboarding en Runn. Parámetro opcional ?date=YYYY-MM-DD
    """
    t0 = time.time()
    reference = _parse_yyyy_mm_dd(request.args.get("date"))
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
        print("runn_onboarding error:", repr(exc))
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
    """
    Sincroniza ausencias en Runn. Parámetro opcional ?date=YYYY-MM-DD
    """
    t0 = time.time()
    reference = _parse_yyyy_mm_dd(request.args.get("date"))
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
        print("runn_timeoff error:", repr(exc))
        return _json_ok(
            {
                "status": "error",
                "message": str(exc),
                "reference_date": reference.isoformat() if reference else None,
            },
            500,
        )
