from __future__ import annotations

import datetime as dt
import logging
from typing import Any, Dict, List, Optional

from app.clients.charthop import (
    ch_fetch_timeoff_enriched,
    ch_get_timeoff,
    ch_get_person,
    ch_people_starting_between,
    ch_person_primary_email,
    _person_email,
    ch_fetch_people_by_ids,
)
from app.clients.runn import (
    runn_create_timeoff,
    runn_find_person_by_email,
    runn_upsert_person,
    runn_get_existing_leave,
    runn_list_person_timeoffs,
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
    return value[:10]


def _timeoff_category(entry: Dict[str, Any]) -> str:
    """
    Decide la categoría para mapear al endpoint correcto de Runn v1.0:
    - "leave" -> /time-offs/leave
    - "holidays" -> /time-offs/holidays
    - "rostered-off" -> /time-offs/rostered-off
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

    if "holiday" in text or "feriado" in text or "public" in text:
        return "holidays"
    if "roster" in text or "rostered" in text or "floating" in text or "lieu" in text:
        return "rostered-off"
    
    # Default: leave (incluye vacation, sick, PTO, etc.)
    return "leave"


def _timeoff_reason(entry: Dict[str, Any]) -> str:
    """Extrae la razón/nota del time-off."""
    fields = entry.get("fields") or {}
    raw_reason = (fields.get("reason") or entry.get("reason") or "").strip()
    raw_type = (fields.get("type") or entry.get("type") or "").strip()
    policy = (fields.get("policy") or "").strip()
    
    for s in (raw_reason, raw_type, policy):
        if s:
            return s
    return "Time Off"


# -------------------------
# Onboarding
# -------------------------

def sync_runn_onboarding(reference: dt.date | None = None) -> Dict[str, Any]:
    """
    Sincroniza personas que comienzan dentro de la ventana de lookahead.
    """
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
            results.append({
                "person": name,
                "status": "skipped",
                "reason": "missing email"
            })
            continue
        
        # employment_type se mapea a role en Runn
        employment_type = fields.get("employment type") or "employee"
        
        runn_resp = runn_upsert_person(
            name=name or email,
            email=email,
            employment_type=employment_type,
            starts_at=start_date or reference.isoformat(),
        )
        
        results.append({
            "person": name or email,
            "status": "created" if runn_resp else "error",
            "response": runn_resp
        })
    
    return {
        "processed": len(people),
        "results": results
    }


def sync_runn_onboarding_event(person_id: str) -> Dict[str, Any]:
    """Procesa un evento puntual de persona desde ChartHop."""
    person_id = (person_id or "").strip()
    if not person_id:
        return {"status": "error", "reason": "missing person_id"}

    person = ch_get_person(person_id)
    if not person:
        return {
            "status": "error",
            "reason": "person not found",
            "person_id": person_id
        }

    fields = person.get("fields") if isinstance(person.get("fields"), dict) else {}
    email = ch_person_primary_email(person)
    
    if not email:
        return {
            "status": "skipped",
            "reason": "missing email",
            "person_id": person_id
        }

    name_parts = [
        (fields.get("name") or "").strip(),
        " ".join(
            part.strip()
            for part in [
                fields.get("name first") or "",
                fields.get("name last") or ""
            ]
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
# Time off (v1.0)
# -------------------------

def _check_existing_timeoff(
    person_id: int,
    start_date: str,
    end_date: str,
    category: str
) -> Optional[Dict[str, Any]]:
    """
    Verifica si ya existe un time-off similar.
    En v1.0, la API hace merge automático de periodos que se traslapan,
    pero igual verificamos para logging.
    """
    # Listar time-offs existentes del tipo correcto
    existing = runn_list_person_timeoffs(person_id, category)
    
    for to in existing:
        # Verificar si hay overlap de fechas
        to_start = to.get("startDate", "")
        to_end = to.get("endDate", "")
        
        # Overlap si:
        # - El nuevo empieza antes de que termine el existente
        # - El nuevo termina después de que empiece el existente
        if to_start <= end_date and to_end >= start_date:
            return to
    
    return None


def _sync_timeoff_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    """
    Sincroniza un registro de time-off de ChartHop a Runn v1.0.
    
    IMPORTANTE: Runn v1.0 hace merge automático de periodos que se traslapan,
    así que no necesitamos preocuparnos tanto por duplicados exactos.
    """
    fields = entry.get("fields") or {}

    # 1) Obtener email
    email = (entry.get("personEmail") or "").strip()
    if not email:
        email = (
            fields.get("person contact workemail") or 
            fields.get("contact workemail") or ""
        ).strip()
    
    if not email:
        email = fields.get("person contact personalemail", "").strip()
    
    # Fallback por personId usando v1 (más confiable para emails)
    if not email:
        person_id = str(
            entry.get("personId")
            or (entry.get("person") or {}).get("id")
            or ""
        ).strip()
        
        if person_id:
            pmap = ch_fetch_people_by_ids([person_id])
            pdata = pmap.get(person_id) or {}
            candidate = (pdata.get("email") or "").strip()
            if candidate:
                email = candidate
            else:
                # Último fallback con v2
                person_v2 = ch_get_person(person_id)
                if person_v2:
                    candidate = _person_email(person_v2) or ""
                    if candidate:
                        email = candidate

    if not email:
        logger.warning(
            "Timeoff skipped: missing email",
            extra={
                "timeoffId": entry.get("id"),
                "personId": entry.get("personId")
            }
        )
        return {
            "status": "skipped",
            "reason": "missing email",
            "entry_id": entry.get("id")
        }

    # 2) Buscar persona en Runn
    person = runn_find_person_by_email(email)
    if not person or not person.get("id"):
        return {
            "status": "skipped",
            "reason": "person not found in Runn",
            "email": email
        }

    # 3) Fechas
    start_date = _safe_date(
        fields.get("start date") or 
        entry.get("startDate") or 
        ""
    )
    end_date = _safe_date(
        fields.get("end date") or 
        entry.get("endDate") or 
        start_date
    )
    
    if not start_date:
        return {
            "status": "skipped",
            "reason": "missing start date",
            "email": email
        }

    # 4) Determinar categoría (leave, holidays, rostered-off)
    category = _timeoff_category(entry)
    reason = _timeoff_reason(entry)
    ext_id = str(entry.get("id") or fields.get("id") or "")
    note = f"ChartHop:{ext_id} • {reason}" if ext_id or reason else None

    # 5) Verificar si ya existe (para logging)
    existing = _check_existing_timeoff(
        person_id=int(person["id"]),
        start_date=start_date,
        end_date=end_date or start_date,
        category=category
    )
    
    if existing:
        logger.info(
            f"Time-off overlaps with existing entry (Runn will merge automatically): "
            f"{email} {start_date} to {end_date}"
        )
        # No retornamos, dejamos que Runn haga el merge

    # 6) Crear en Runn v1.0
    # La API hace merge automático si hay overlap
    success = runn_create_timeoff(
        person_id=int(person["id"]),
        start_date=start_date,
        end_date=end_date or start_date,
        category=category,
        note=note,
        reason=reason,
    )

    return {
        "status": "synced" if success else "error",
        "email": email,
        "category": category,
        "endpoint": f"/time-offs/{category}",
        "runn_person_id": person.get("id"),
        "start_date": start_date,
        "end_date": end_date or start_date,
        "ext_ref": ext_id,
        "auto_merged": existing is not None,
    }


def sync_runn_timeoff(reference: dt.date | None = None) -> Dict[str, Any]:
    """
    Sincroniza time-off de ChartHop a Runn dentro de la ventana configurada.
    Usa la API v1.0 que hace merge automático de periodos overlapping.
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
        "synced": sum(1 for r in results if r.get("status") == "synced"),
        "skipped": sum(1 for r in results if r.get("status") == "skipped"),
        "error": sum(1 for r in results if r.get("status") == "error"),
        "auto_merged": sum(1 for r in results if r.get("auto_merged")),
        "results": results,
    }
    
    logger.info(
        "Runn timeoff sync summary",
        extra={
            "processed": summary["processed"],
            "synced": summary["synced"],
            "skipped": summary["skipped"],
            "error": summary["error"],
            "auto_merged": summary["auto_merged"],
        }
    )
    
    return summary


def sync_runn_timeoff_event(timeoff_id: str) -> Dict[str, Any]:
    """
    Procesa un evento puntual de time-off desde webhook de ChartHop.
    Usa la API v1.0 de Runn con merge automático.
    """
    entry = ch_get_timeoff(timeoff_id)
    if not entry:
        return {
            "status": "error",
            "reason": "timeoff not found",
            "timeoff_id": timeoff_id
        }
    
    result = _sync_timeoff_entry(entry)
    result.setdefault("timeoff_id", entry.get("id") or timeoff_id)
    
    return result
