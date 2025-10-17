from __future__ import annotations

import datetime as dt
import logging
from typing import Any, Dict, List, Optional

from app.clients.charthop import (
    ch_fetch_timeoff_enriched,
    ch_get_timeoff,
    ch_get_person,               # v2
    ch_people_starting_between,
    ch_person_primary_email,
    _person_email,               # helper para v2
    ch_fetch_people_by_ids,      # <-- v1: include=contact,contacts (fallback robusto)
)
from app.clients.runn import (
    runn_create_timeoff,
    runn_find_person_by_email,
    runn_upsert_person,
)
from app.utils.config import (
    RUNN_ONBOARDING_LOOKAHEAD_DAYS,
    RUNN_TIMEOFF_LOOKAHEAD_DAYS,
    RUNN_TIMEOFF_LOOKBACK_DAYS,
)

logger = logging.getLogger(__name__)

# -------------------------
# Utilidades
# -------------------------

def _safe_date(value: str) -> str:
    if not value:
        return ""
    # Normaliza a "YYYY-MM-DD"
    return value[:10]

def _parse_hours_per_day(entry: Dict[str, Any]) -> float:
    fields = entry.get("fields") or {}
    # intenta varios nombres que suelen aparecer en ChartHop
    cand = (
        entry.get("hoursPerDay")
        or fields.get("hours per day")
        or fields.get("hours/day")
        or fields.get("hours_per_day")
    )
    try:
        return float(cand) if cand is not None else 8.0
    except Exception:
        return 8.0

def _timeoff_category(entry: Dict[str, Any]) -> str:
    """
    Decide la categoría en Runn:
      - "holidays" si parece feriado/holiday
      - "rostered-off" si menciona roster/rostered/floating
      - por defecto "leave"
    """
    fields = entry.get("fields") or {}
    text = " ".join(
        str(x or "")
        for x in [
            fields.get("type"),
            fields.get("reason"),
            entry.get("type"),
            entry.get("reason"),
            fields.get("policy"),
        ]
    ).lower()

    if "holiday" in text or "feriado" in text:
        return "holidays"
    if "roster" in text or "rostered" in text or "floating" in text:
        return "rostered-off"
    return "leave"

def _timeoff_reason(entry: Dict[str, Any]) -> str:
    fields = entry.get("fields") or {}
    raw_reason = (fields.get("reason") or entry.get("reason") or "").strip()
    raw_type = (fields.get("type") or entry.get("type") or "").strip()
    policy = (fields.get("policy") or "").strip()
    # pref: reason > type > policy
    for s in (raw_reason, raw_type, policy):
        if s:
            return s
    return "Leave"

# -------------------------
# Onboarding (sin cambios funcionales)
# -------------------------

def sync_runn_onboarding(reference: dt.date | None = None) -> Dict[str, Any]:
    reference = reference or dt.date.today()
    end = reference + dt.timedelta(days=RUNN_ONBOARDING_LOOKAHEAD_DAYS)
    people = ch_people_starting_between(reference, end)
    results: List[Dict[str, Any]] = []
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


def sync_runn_onboarding_event(person_id: str) -> Dict[str, Any]:
    """Procesa un evento puntual de persona (create/update) desde ChartHop."""

    person_id = (person_id or "").strip()
    if not person_id:
        return {"status": "error", "reason": "missing person_id"}

    person = ch_get_person(person_id)
    if not person:
        return {"status": "error", "reason": "person not found", "person_id": person_id}

    fields = person.get("fields") if isinstance(person.get("fields"), dict) else {}
    email = ch_person_primary_email(person)
    if not email:
        return {"status": "skipped", "reason": "missing email", "person_id": person_id}

    name_parts = [
        (fields.get("name") or "").strip(),
        " ".join(
            part.strip()
            for part in [fields.get("name first") or "", fields.get("name last") or ""]
            if part
        ).strip(),
        (person.get("name") or "").strip(),
    ]
    name = next((part for part in name_parts if part), email)

    employment_type = (
        (fields.get("employment type") or "").strip()
        or (fields.get("employmenttype") or "").strip()
        or (person.get("employmentType") or "").strip()
        or "employee"
    )

    start_candidate = (
        fields.get("start date")
        or fields.get("startdate")
        or fields.get("start date org")
        or person.get("startDateOrg")
        or ""
    )
    starts_at = _safe_date(start_candidate)

    runn_resp = runn_upsert_person(
        name=name,
        email=email,
        employment_type=employment_type or "employee",
        starts_at=starts_at or None,
    )

    result: Dict[str, Any] = {
        "status": "synced" if runn_resp else "error",
        "person_id": person_id,
        "email": email,
        "employment_type": employment_type or "employee",
        "name": name,
    }
    if starts_at:
        result["starts_at"] = starts_at
    if runn_resp is not None:
        result["runn_response"] = runn_resp
    return result

# -------------------------
# Time off (v1)
# -------------------------

def _sync_timeoff_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    """
    Toma un registro de ChartHop y crea el time-off en Runn v1 (idempotencia básica por note).
    """
    fields = entry.get("fields") or {}

    # 1) Email (directo del evento)
    email = (entry.get("personEmail") or "").strip()
    if not email:
        email = (fields.get("person contact workemail") or fields.get("contact workemail") or "").strip()
    if not email:
        email = (fields.get("person contact personalemail") or "").strip()

    # 1b) Fallback por personId usando v1 person (contacts + contact)
    if not email:
        person_id = str(
            entry.get("personId")
            or (entry.get("person") or {}).get("id")
            or ""
        ).strip()
        if person_id:
            # v1: include=contact,contacts – más confiable para emails
            pmap = ch_fetch_people_by_ids([person_id])
            pdata = pmap.get(person_id) or {}
            candidate = (pdata.get("email") or "").strip()
            if candidate:
                email = candidate
                entry = dict(entry)
                entry.setdefault("personEmail", email)

    # 1c) Fallback adicional con v2 get person (por si en algún caso v1 no trae nada)
    if not email:
        person_id = str(
            entry.get("personId")
            or (entry.get("person") or {}).get("id")
            or ""
        ).strip()
        if person_id:
            person_v2 = ch_get_person(person_id)  # incluye fields,contacts
            if person_v2:
                candidate = _person_email(person_v2) or ""
                if candidate:
                    email = candidate
                    entry = dict(entry)
                    entry.setdefault("personEmail", email)

    if not email:
        logger.warning(
            "Timeoff skipped: missing email",
            extra={"timeoffId": entry.get("id"), "personId": entry.get("personId")},
        )
        return {"status": "skipped", "reason": "missing email", "entry": entry}

    person = runn_find_person_by_email(email)
    if not person or not person.get("id"):
        return {"status": "skipped", "reason": "person not found", "email": email}

    # 2) Fechas
    start_date = _safe_date(fields.get("start date") or entry.get("startDate") or "")
    end_date = _safe_date(fields.get("end date") or entry.get("endDate") or start_date)
    if not start_date:
        return {"status": "skipped", "reason": "missing start date", "email": email}

    # 3) Duración por día
    hours_per_day = _parse_hours_per_day(entry)  # default 8.0
    minutes_per_day = int(round(hours_per_day * 60))

    # 4) Categoría y nota (idempotencia por note con ref externa)
    category = _timeoff_category(entry)  # "leave" | "holidays" | "rostered-off"
    reason = _timeoff_reason(entry)
    ext_id = str(entry.get("id") or fields.get("id") or "")
    note = f"CHP:{ext_id} • {reason}" if ext_id or reason else None

    # 5) Create en Runn (v1)
    ok = runn_create_timeoff(
        person_id=int(person["id"]),
        start_date=start_date,
        end_date=end_date or start_date,
        minutes_per_day=minutes_per_day,
        note=note,
        category=category,
    )
    return {
        "status": "synced" if ok else "error",
        "email": email,
        "category": category,
        "minutesPerDay": minutes_per_day,
        "runn_person_id": person.get("id"),
        "start_date": start_date,
        "end_date": end_date or start_date,
        "ext_ref": ext_id,
    }

def sync_runn_timeoff(reference: dt.date | None = None) -> Dict[str, Any]:
    """
    Trae time-off de ChartHop y los inserta en Runn v1.
    Ventana: [reference - LOOKBACK, reference + LOOKAHEAD]
    """
    reference = reference or dt.date.today()
    start = reference - dt.timedelta(days=RUNN_TIMEOFF_LOOKBACK_DAYS)
    end = reference + dt.timedelta(days=RUNN_TIMEOFF_LOOKAHEAD_DAYS)

    events = ch_fetch_timeoff_enriched(start.isoformat(), end.isoformat())
    results: List[Dict[str, Any]] = []
    for entry in events:
        results.append(_sync_timeoff_entry(entry))

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

def sync_runn_timeoff_event(timeoff_id: str) -> Dict[str, Any]:
    """
    Para llamadas puntuales (webhook) por ID de ChartHop.
    """
    entry = ch_get_timeoff(timeoff_id)
    if not entry:
        return {"status": "error", "reason": "timeoff not found", "timeoff_id": timeoff_id}
    result = _sync_timeoff_entry(entry)
    result.setdefault("timeoff_id", entry.get("id") or timeoff_id)
    return result
