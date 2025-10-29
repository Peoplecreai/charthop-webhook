from __future__ import annotations

import datetime as dt
import logging
import os
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
    runn_delete_timeoff,
    runn_find_person_by_email,
    runn_update_timeoff,
    runn_upsert_person,
    runn_get_existing_leave,
    runn_list_person_timeoffs,
)
from app.utils.timeoff_mapping import get_timeoff_mapping
from app.utils.sync_metrics import get_sync_metrics
from app.utils.config import (
    RUNN_ONBOARDING_LOOKAHEAD_DAYS,
    RUNN_TIMEOFF_LOOKAHEAD_DAYS,
    RUNN_TIMEOFF_LOOKBACK_DAYS,
)

logger = logging.getLogger(__name__)

# -------------------------
# Utilidades
# -------------------------

def _safe_date(value: str) -> Optional[str]:
    """
    Valida y normaliza una fecha a formato YYYY-MM-DD.

    Args:
        value: Fecha en string (puede ser ISO 8601 completo)

    Returns:
        Fecha en formato YYYY-MM-DD o None si es inválida
    """
    if not value:
        return None

    date_str = value[:10]

    # Validar formato YYYY-MM-DD
    try:
        dt.datetime.strptime(date_str, "%Y-%m-%d")
        return date_str
    except ValueError:
        logger.warning(f"Invalid date format: {value}")
        return None


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


def _should_skip_timeoff(entry: Dict[str, Any]) -> Optional[str]:
    """
    Verifica si un time-off debe saltarse según su estado.

    Args:
        entry: Entrada de time-off de ChartHop

    Returns:
        Razón para saltarlo o None si debe procesarse
    """
    fields = entry.get("fields") or {}

    # Verificar estado de aprobación
    status = (
        entry.get("status") or
        fields.get("status") or
        entry.get("state") or
        fields.get("state") or
        ""
    ).lower()

    # Estados que no deben sincronizarse
    skip_statuses = {
        "denied", "rejected", "cancelled", "canceled",
        "draft", "pending", "withdrawn"
    }

    for skip_status in skip_statuses:
        if skip_status in status:
            return f"status is {status}"

    # Verificar si está cancelado explícitamente
    cancelled = (
        entry.get("cancelled") or
        entry.get("canceled") or
        fields.get("cancelled") or
        fields.get("canceled")
    )

    if cancelled is True or str(cancelled).lower() == "true":
        return "time off is cancelled"

    # Verificar si está activo (si el campo existe)
    active = entry.get("active") or fields.get("active")
    if active is False or str(active).lower() == "false":
        return "time off is inactive"

    return None


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
    ext_id = str(entry.get("id") or fields.get("id") or "")

    # 0) Validar estado del time-off
    skip_reason = _should_skip_timeoff(entry)
    if skip_reason:
        logger.info(
            f"Timeoff skipped: {skip_reason}",
            extra={"timeoffId": ext_id}
        )
        return {
            "status": "skipped",
            "reason": skip_reason,
            "entry_id": ext_id
        }

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
    note = f"ChartHop:{ext_id} • {reason}" if ext_id or reason else None

    # 5) Verificar si ya existe un mapeo (para updates)
    mapping = get_timeoff_mapping()
    existing_mapping = mapping.get_runn_id(ext_id) if ext_id else None

    if existing_mapping:
        # Ya existe, actualizar en lugar de crear
        runn_id = existing_mapping["runn_id"]
        existing_category = existing_mapping["category"]

        logger.info(
            f"Time-off already mapped: ChartHop {ext_id} -> Runn {runn_id}, updating"
        )

        # Actualizar
        updated = runn_update_timeoff(
            timeoff_id=runn_id,
            category=existing_category,
            start_date=start_date,
            end_date=end_date or start_date,
            note=note,
        )

        return {
            "status": "updated" if updated else "error",
            "email": email,
            "category": existing_category,
            "runn_person_id": person.get("id"),
            "runn_timeoff_id": runn_id,
            "start_date": start_date,
            "end_date": end_date or start_date,
            "ext_ref": ext_id,
        }

    # 6) Verificar si ya existe otro time-off en las mismas fechas (para logging)
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

    # 7) Crear en Runn v1.0
    # La API hace merge automático si hay overlap
    runn_response = runn_create_timeoff(
        person_id=int(person["id"]),
        start_date=start_date,
        end_date=end_date or start_date,
        category=category,
        note=note,
        reason=reason,
    )

    if runn_response:
        # Guardar mapeo para futuras actualizaciones/eliminaciones
        runn_id = runn_response.get("id")
        if runn_id and ext_id:
            mapping.add(
                charthop_id=ext_id,
                runn_id=runn_id,
                category=category,
                person_email=email
            )

    return {
        "status": "synced" if runn_response else "error",
        "email": email,
        "category": category,
        "endpoint": f"/time-offs/{category}",
        "runn_person_id": person.get("id"),
        "runn_timeoff_id": runn_response.get("id") if runn_response else None,
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
    metrics = get_sync_metrics()
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
        "updated": sum(1 for r in results if r.get("status") == "updated"),
        "skipped": sum(1 for r in results if r.get("status") == "skipped"),
        "error": sum(1 for r in results if r.get("status") == "error"),
        "auto_merged": sum(1 for r in results if r.get("auto_merged")),
        "results": results,
    }

    # Actualizar métricas
    metrics.increment_counter("timeoff_synced", summary["synced"])
    metrics.increment_counter("timeoff_updated", summary["updated"])
    metrics.increment_counter("timeoff_skipped", summary["skipped"])
    metrics.increment_counter("timeoff_errors", summary["error"])
    metrics.record_sync("timeoff_batch")

    # Registrar errores
    for result in results:
        if result.get("status") == "error":
            metrics.record_error(
                error_type="timeoff",
                error_message=result.get("reason", "unknown error"),
                entity_id=result.get("entry_id")
            )

    logger.info(
        "Runn timeoff sync summary",
        extra={
            "processed": summary["processed"],
            "synced": summary["synced"],
            "updated": summary["updated"],
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
    metrics = get_sync_metrics()

    entry = ch_get_timeoff(timeoff_id)
    if not entry:
        error_result = {
            "status": "error",
            "reason": "timeoff not found",
            "timeoff_id": timeoff_id
        }
        metrics.increment_counter("timeoff_errors")
        metrics.record_error(
            error_type="timeoff",
            error_message="timeoff not found",
            entity_id=timeoff_id
        )
        return error_result

    result = _sync_timeoff_entry(entry)
    result.setdefault("timeoff_id", entry.get("id") or timeoff_id)

    # Actualizar métricas
    status = result.get("status")
    if status == "synced":
        metrics.increment_counter("timeoff_synced")
    elif status == "updated":
        metrics.increment_counter("timeoff_updated")
    elif status == "skipped":
        metrics.increment_counter("timeoff_skipped")
    elif status == "error":
        metrics.increment_counter("timeoff_errors")
        metrics.record_error(
            error_type="timeoff",
            error_message=result.get("reason", "unknown error"),
            entity_id=timeoff_id
        )

    metrics.record_sync("timeoff_event")

    return result


def delete_runn_timeoff_event(timeoff_id: str) -> Dict[str, Any]:
    """
    Elimina un time-off de Runn cuando se elimina en ChartHop.

    Args:
        timeoff_id: ID del time-off en ChartHop

    Returns:
        Resultado de la operación
    """
    metrics = get_sync_metrics()

    timeoff_id = (timeoff_id or "").strip()
    if not timeoff_id:
        error_result = {"status": "error", "reason": "missing timeoff_id"}
        metrics.increment_counter("timeoff_errors")
        metrics.record_error(
            error_type="timeoff_delete",
            error_message="missing timeoff_id"
        )
        return error_result

    # Buscar el mapeo
    mapping = get_timeoff_mapping()
    mapping_info = mapping.get_runn_id(timeoff_id)

    if not mapping_info:
        # No existe mapeo, probablemente nunca se sincronizó
        logger.info(f"No mapping found for ChartHop timeoff {timeoff_id}, nothing to delete")
        metrics.increment_counter("timeoff_skipped")
        return {
            "status": "skipped",
            "reason": "no mapping found",
            "timeoff_id": timeoff_id
        }

    runn_id = mapping_info["runn_id"]
    category = mapping_info["category"]

    # Eliminar en Runn
    deleted = runn_delete_timeoff(runn_id, category)

    if deleted:
        # Eliminar el mapeo
        mapping.remove(timeoff_id)

        metrics.increment_counter("timeoff_deleted")
        metrics.record_sync("timeoff_delete")

        return {
            "status": "deleted",
            "timeoff_id": timeoff_id,
            "runn_timeoff_id": runn_id,
            "category": category
        }
    else:
        metrics.increment_counter("timeoff_errors")
        metrics.record_error(
            error_type="timeoff_delete",
            error_message="failed to delete from Runn",
            entity_id=timeoff_id
        )
        return {
            "status": "error",
            "reason": "failed to delete from Runn",
            "timeoff_id": timeoff_id,
            "runn_timeoff_id": runn_id
        }


# -------------------------
# Compensación / Cost Per Hour
# -------------------------

# Horas anuales usadas para convertir CTC a costPerHour (configurable via env)
RUNN_ANNUAL_HOURS = float(
    os.getenv(
        "RUNN_ANNUAL_HOURS",
        os.getenv("ANNUAL_EFFECTIVE_HOURS", "1856"),
    )
)


def _calculate_cost_per_hour(cost_to_company: float) -> float:
    """
    Calcula el costo por hora dado el CosttoCompany anualizado.

    Formula: costPerHour = CosttoCompany / RUNN_ANNUAL_HOURS

    Args:
        cost_to_company: Costo anual total (CosttoCompany de ChartHop)

    Returns:
        Costo por hora redondeado a 2 decimales
    """
    if cost_to_company <= 0:
        return 0.0

    if RUNN_ANNUAL_HOURS <= 0:
        return 0.0

    cost_per_hour = cost_to_company / RUNN_ANNUAL_HOURS
    return round(cost_per_hour, 2)


def sync_runn_compensation_event(person_id: str) -> Dict[str, Any]:
    """
    Sincroniza la compensación de una persona desde ChartHop a Runn.

    Proceso:
    1. Obtener CosttoCompany de ChartHop
    2. Calcular costPerHour = CosttoCompany / RUNN_ANNUAL_HOURS
    3. Buscar persona en Runn por email
    4. Obtener contratos activos
    5. Actualizar costPerHour en cada contrato activo

    Args:
        person_id: ID de la persona en ChartHop

    Returns:
        {
            "status": "synced" | "skipped" | "error",
            "person_id": "abc123",
            "email": "user@example.com",
            "cost_to_company": 100000.0,
            "cost_per_hour": 53.88,
            "contracts_updated": 2,
            "runn_person_id": 456
        }
    """
    from app.clients.charthop import ch_get_person_compensation
    from app.clients.runn import (
        runn_find_person_by_email,
        runn_get_active_contracts,
        runn_update_contract_cost,
    )

    metrics = get_sync_metrics()

    person_id = (person_id or "").strip()
    if not person_id:
        error_result = {"status": "error", "reason": "missing person_id"}
        metrics.increment_counter("compensation_errors")
        metrics.record_error(
            error_type="compensation",
            error_message="missing person_id"
        )
        return error_result

    # 1. Obtener compensación de ChartHop
    comp_data = ch_get_person_compensation(person_id)

    if not comp_data:
        skip_result = {
            "status": "skipped",
            "reason": "person not found in ChartHop",
            "person_id": person_id
        }
        metrics.increment_counter("compensation_skipped")
        return skip_result

    email = comp_data.get("email", "")
    cost_to_company = comp_data.get("cost_to_company")
    job_id = comp_data.get("job_id")

    if not email:
        skip_result = {
            "status": "skipped",
            "reason": "missing email",
            "person_id": person_id,
            "job_id": job_id,
        }
        metrics.increment_counter("compensation_skipped")
        return skip_result

    if cost_to_company is None or cost_to_company <= 0:
        skip_result = {
            "status": "skipped",
            "reason": "missing or invalid cost to company",
            "person_id": person_id,
            "email": email,
            "job_id": job_id
        }
        metrics.increment_counter("compensation_skipped")
        return skip_result

    # 2. Calcular cost per hour
    cost_per_hour = _calculate_cost_per_hour(cost_to_company)

    if cost_per_hour <= 0:
        skip_result = {
            "status": "skipped",
            "reason": "calculated cost per hour is invalid",
            "person_id": person_id,
            "email": email,
            "job_id": job_id,
            "cost_to_company": cost_to_company
        }
        metrics.increment_counter("compensation_skipped")
        return skip_result

    # 3. Buscar persona en Runn
    runn_person = runn_find_person_by_email(email)

    if not runn_person or not runn_person.get("id"):
        skip_result = {
            "status": "skipped",
            "reason": "person not found in Runn",
            "person_id": person_id,
            "email": email,
            "job_id": job_id
        }
        metrics.increment_counter("compensation_skipped")
        return skip_result

    runn_person_id = int(runn_person["id"])

    # 4. Obtener contratos activos
    active_contracts = runn_get_active_contracts(runn_person_id)

    if not active_contracts:
        skip_result = {
            "status": "skipped",
            "reason": "no active contracts",
            "person_id": person_id,
            "email": email,
            "job_id": job_id,
            "runn_person_id": runn_person_id
        }
        metrics.increment_counter("compensation_skipped")
        return skip_result

    # 5. Actualizar cada contrato activo
    contracts_updated = 0
    contracts_failed = 0

    for contract in active_contracts:
        contract_id = contract.get("id")
        if not contract_id:
            continue

        # Verificar si ya tiene el mismo costo (evitar updates innecesarios)
        current_cost = contract.get("costPerHour")
        if current_cost is not None:
            try:
                current_cost_float = float(current_cost)
            except (TypeError, ValueError):
                current_cost_float = None
            else:
                if abs(current_cost_float - cost_per_hour) < 0.01:
                    logger.info(
                        "Contract %s already has cost %.2f/hour (difference < 0.01), skipping",
                        contract_id,
                        current_cost_float,
                    )
                    continue

        result = runn_update_contract_cost(contract_id, cost_per_hour)

        if result:
            contracts_updated += 1
            metrics.increment_counter("contracts_updated")
        else:
            contracts_failed += 1

    # Determinar status final
    if contracts_updated > 0:
        status = "synced"
        metrics.increment_counter("compensation_synced")
    elif contracts_failed > 0:
        status = "error"
        metrics.increment_counter("compensation_errors")
        metrics.record_error(
            error_type="compensation",
            error_message=f"failed to update {contracts_failed} contracts",
            entity_id=person_id
        )
    else:
        status = "skipped"
        metrics.increment_counter("compensation_skipped")

    metrics.record_sync("compensation_event")

    return {
        "status": status,
        "person_id": person_id,
        "email": email,
        "job_id": job_id,
        "name": comp_data.get("name"),
        "cost_to_company": cost_to_company,
        "currency": comp_data.get("currency", "USD"),
        "cost_per_hour": cost_per_hour,
        "runn_person_id": runn_person_id,
        "contracts_updated": contracts_updated,
        "contracts_failed": contracts_failed,
        "total_active_contracts": len(active_contracts),
    }


def sync_runn_compensation(reference: dt.date | None = None) -> Dict[str, Any]:
    """
    Sincronización batch de compensaciones.

    Procesa todas las personas activas en ChartHop con compensación
    y actualiza sus contratos activos en Runn.

    Args:
        reference: Fecha de referencia para determinar contratos activos (default: hoy)

    Returns:
        {
            "processed": 150,
            "synced": 145,
            "skipped": 3,
            "error": 2,
            "total_contracts_updated": 200,
            "results": [...]
        }
    """
    from app.clients.charthop import ch_fetch_people_with_compensation
    from app.clients.runn import (
        runn_find_person_by_email,
        runn_get_active_contracts,
        runn_update_contract_cost,
    )

    metrics = get_sync_metrics()
    reference = reference or dt.date.today()
    reference_str = reference.isoformat()

    # Obtener todas las personas con compensación de ChartHop
    people = ch_fetch_people_with_compensation(active_only=True)

    results: List[Dict[str, Any]] = []
    total_contracts_updated = 0

    for person_data in people:
        person_id = person_data.get("person_id", "")
        email = person_data.get("email", "")
        cost_to_company = person_data.get("cost_to_company")

        if not email:
            results.append({
                "person_id": person_id,
                "status": "skipped",
                "reason": "missing email"
            })
            continue

        if cost_to_company is None or cost_to_company <= 0:
            results.append({
                "person_id": person_id,
                "email": email,
                "status": "skipped",
                "reason": "missing or invalid cost to company"
            })
            continue

        # Calcular cost per hour
        cost_per_hour = _calculate_cost_per_hour(cost_to_company)

        if cost_per_hour <= 0:
            results.append({
                "person_id": person_id,
                "email": email,
                "status": "skipped",
                "reason": "invalid cost per hour"
            })
            continue

        # Buscar en Runn
        runn_person = runn_find_person_by_email(email)

        if not runn_person:
            results.append({
                "person_id": person_id,
                "email": email,
                "status": "skipped",
                "reason": "person not found in Runn"
            })
            continue

        runn_person_id = int(runn_person["id"])

        # Obtener contratos activos
        active_contracts = runn_get_active_contracts(
            runn_person_id,
            reference_date=reference_str
        )

        if not active_contracts:
            results.append({
                "person_id": person_id,
                "email": email,
                "runn_person_id": runn_person_id,
                "status": "skipped",
                "reason": "no active contracts"
            })
            continue

        # Actualizar contratos
        contracts_updated = 0
        contracts_failed = 0

        for contract in active_contracts:
            contract_id = contract.get("id")
            if not contract_id:
                continue

            # Verificar si ya tiene el mismo costo
            current_cost = contract.get("costPerHour")
            if current_cost is not None:
                current_cost = round(float(current_cost), 2)
                if current_cost == cost_per_hour:
                    continue

            result = runn_update_contract_cost(contract_id, cost_per_hour)

            if result:
                contracts_updated += 1
            else:
                contracts_failed += 1

        total_contracts_updated += contracts_updated

        # Status
        if contracts_updated > 0:
            status = "synced"
        elif contracts_failed > 0:
            status = "error"
        else:
            status = "skipped"

        results.append({
            "person_id": person_id,
            "email": email,
            "name": person_data.get("name"),
            "status": status,
            "cost_to_company": cost_to_company,
            "cost_per_hour": cost_per_hour,
            "runn_person_id": runn_person_id,
            "contracts_updated": contracts_updated,
            "contracts_failed": contracts_failed,
        })

    summary = {
        "processed": len(people),
        "synced": sum(1 for r in results if r.get("status") == "synced"),
        "skipped": sum(1 for r in results if r.get("status") == "skipped"),
        "error": sum(1 for r in results if r.get("status") == "error"),
        "total_contracts_updated": total_contracts_updated,
        "reference_date": reference_str,
        "results": results,
    }

    # Actualizar métricas
    metrics.increment_counter("compensation_synced", summary["synced"])
    metrics.increment_counter("compensation_skipped", summary["skipped"])
    metrics.increment_counter("compensation_errors", summary["error"])
    metrics.increment_counter("contracts_updated", total_contracts_updated)
    metrics.record_sync("compensation_batch")

    # Registrar errores
    for result in results:
        if result.get("status") == "error":
            metrics.record_error(
                error_type="compensation",
                error_message=result.get("reason", "unknown error"),
                entity_id=result.get("person_id")
            )

    logger.info(
        "Runn compensation sync summary",
        extra={
            "processed": summary["processed"],
            "synced": summary["synced"],
            "skipped": summary["skipped"],
            "error": summary["error"],
            "contracts_updated": total_contracts_updated,
        }
    )

    return summary
