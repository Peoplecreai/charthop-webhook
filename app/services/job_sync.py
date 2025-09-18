from __future__ import annotations

from typing import Optional

from app.clients.charthop import ch_find_job, ch_upsert_job_field
from app.clients.teamtailor import (
    tt_create_job_from_ch,
    tt_update_job,
    tt_upsert_job_custom_field,
)
from app.utils.config import CH_CF_JOB_TT_ID_LABEL


def _extract_job_title(job_payload: dict) -> str:
    title = job_payload.get("title") or ""
    if title:
        return title
    fields = job_payload.get("fields") or {}
    return fields.get("title") or fields.get("name") or "Untitled"


def _extract_job_open(job_payload: dict) -> Optional[bool]:
    if "open" in job_payload:
        return bool(job_payload.get("open"))
    fields = job_payload.get("fields") or {}
    if "open" in fields:
        return bool(fields.get("open"))
    return None


def _status_from_open(is_open: Optional[bool]) -> Optional[str]:
    if is_open is None:
        return None
    return "unlisted" if is_open else "archived"


def sync_job_create(job_id: str) -> Optional[str]:
    job = ch_find_job(job_id)
    if not job:
        print(f"sync_job_create: job {job_id} not found in ChartHop")
        return None

    title = _extract_job_title(job)
    status = _status_from_open(_extract_job_open(job)) or "unlisted"
    resp = tt_create_job_from_ch(title=title, status=status)
    print("sync_job_create TT status:", resp.status_code)
    if not resp.ok:
        return None
    tt_job_id = ((resp.json() or {}).get("data") or {}).get("id")
    if not tt_job_id:
        return None
    try:
        tt_upsert_job_custom_field(tt_job_id, job_id)
    except Exception as exc:  # pragma: no cover - logging
        print("sync_job_create: failed linking TT custom field", repr(exc))
    if CH_CF_JOB_TT_ID_LABEL:
        try:
            ch_upsert_job_field(job_id, CH_CF_JOB_TT_ID_LABEL, tt_job_id)
        except Exception as exc:  # pragma: no cover - logging
            print("sync_job_create: failed writing TT id back to ChartHop", repr(exc))
    return tt_job_id


def sync_job_update(job_id: str) -> bool:
    job = ch_find_job(job_id)
    if not job:
        print(f"sync_job_update: job {job_id} not found in ChartHop")
        return False
    fields = job.get("fields") or {}
    tt_job_id = (fields.get(CH_CF_JOB_TT_ID_LABEL) or "").strip() if CH_CF_JOB_TT_ID_LABEL else ""
    if not tt_job_id:
        print(f"sync_job_update: job {job_id} without TT mapping, skipping")
        return False
    title = _extract_job_title(job)
    status = _status_from_open(_extract_job_open(job))
    resp = tt_update_job(tt_job_id, title=title, status=status)
    if resp is None:
        print(f"sync_job_update: nothing to update for job {tt_job_id}")
        return True
    print("sync_job_update TT status:", resp.status_code)
    return resp.ok
