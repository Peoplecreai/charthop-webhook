from __future__ import annotations

import datetime as dt
import logging
from typing import Dict, List

from app.clients.charthop import (
    ch_fetch_timeoff_enriched,
    ch_get_timeoff,
    ch_people_starting_between,
    ch_person_primary_email,
)
from app.clients.runn import runn_create_leave, runn_find_person_by_email, runn_upsert_person
from app.utils.config import (
    RUNN_ONBOARDING_LOOKAHEAD_DAYS,
    RUNN_TIMEOFF_LOOKAHEAD_DAYS,
    RUNN_TIMEOFF_LOOKBACK_DAYS,
)


logger = logging.getLogger(__name__)


def _safe_date(value: str) -> str:
    if not value:
        return ""
    return value[:10]


def sync_runn_onboarding(reference: dt.date | None = None) -> Dict:
    reference = reference or dt.date.today()
    end = reference + dt.timedelta(days=RUNN_ONBOARDING_LOOKAHEAD_DAYS)
    people = ch_people_starting_between(reference, end)
    results: List[Dict] = []
    for person in people:
        fields = person.get("fields") or {}
        name = " ".join(
            part
            for part in [fields.get("name first"), fields.get("name last")]
            if part
        ).strip() or fields.get("name") or ""
        email = ch_person_primary_email(person)
        start_date = _safe_date(fields.get("start date") or fields.get("startdate") or "")
        if not email:
            results.append({"person": name, "status": "skipped", "reason": "missing email"})
            continue
        runn_resp = runn_upsert_person(
            name=name or email,
            email=email,
            employment_type=fields.get("employment type") or "employee",
            starts_at=start_date or reference.isoformat(),
        )
        results.append({"person": name or email, "status": "created" if runn_resp else "error", "response": runn_resp})
    return {"processed": len(people), "results": results}


def _timeoff_reason(entry: Dict) -> str:
    fields = entry.get("fields") or {}
    raw_reason = (fields.get("reason") or entry.get("reason") or "").lower()
    raw_type = (fields.get("type") or entry.get("type") or "").lower()
    text = raw_reason or raw_type
    if "sick" in text:
        return "Sick leave"
    if "pto" in text or "vacation" in text:
        return "Vacation"
    if "bereavement" in text:
        return "Bereavement"
    return "Leave"


def _sync_timeoff_entry(entry: Dict) -> Dict:
    fields = entry.get("fields") or {}
    email = (entry.get("personEmail") or "").strip()
    if not email:
        email = (fields.get("person contact workemail") or fields.get("contact workemail") or "").strip()
    if not email:
        email = (fields.get("person contact personalemail") or "").strip()
    if not email:
        logger.warning(
            "Timeoff skipped: missing email",
            extra={"timeoffId": entry.get("id"), "personId": entry.get("personId")},
        )
        result = {
            "status": "skipped",
            "reason": "missing email",
            "entry": entry,
        }
        return result
    person = runn_find_person_by_email(email)
    if not person or not person.get("id"):
        return {"status": "skipped", "reason": "person not found", "email": email}
    start_date = _safe_date(fields.get("start date") or entry.get("startDate") or "")
    end_date = _safe_date(fields.get("end date") or entry.get("endDate") or start_date)
    if not start_date:
        return {"status": "skipped", "reason": "missing start date", "email": email}
    reason = _timeoff_reason(entry)
    resp = runn_create_leave(
        person_id=person["id"],
        starts_at=start_date,
        ends_at=end_date or start_date,
        reason=reason,
        external_ref=str(entry.get("id") or fields.get("id") or ""),
    )
    return {"status": "synced" if resp else "error", "email": email, "response": resp}


def sync_runn_timeoff(reference: dt.date | None = None) -> Dict:
    reference = reference or dt.date.today()
    start = reference - dt.timedelta(days=RUNN_TIMEOFF_LOOKBACK_DAYS)
    end = reference + dt.timedelta(days=RUNN_TIMEOFF_LOOKAHEAD_DAYS)
    events = ch_fetch_timeoff_enriched(start.isoformat(), end.isoformat())
    results: List[Dict] = []
    for entry in events:
        result = _sync_timeoff_entry(entry)
        results.append(result)
    summary = {
        "processed": len(events),
        "synced": sum(1 for item in results if item.get("status") == "synced"),
        "skipped": sum(1 for item in results if item.get("status") == "skipped"),
        "error": sum(1 for item in results if item.get("status") == "error"),
        "results": results,
    }
    logger.info(
        "Runn timeoff sync summary",
        extra={
            "processed": summary["processed"],
            "synced": summary["synced"],
            "skipped": summary["skipped"],
            "error": summary["error"],
        },
    )
    return summary


def sync_runn_timeoff_event(timeoff_id: str) -> Dict:
    entry = ch_get_timeoff(timeoff_id)
    if not entry:
        return {"status": "error", "reason": "timeoff not found", "timeoff_id": timeoff_id}
    result = _sync_timeoff_entry(entry)
    result.setdefault("timeoff_id", entry.get("id") or timeoff_id)
    return result
