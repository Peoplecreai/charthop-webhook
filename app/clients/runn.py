from __future__ import annotations

import os
import logging
from typing import Any, Dict, List, Optional

import requests

from app.utils.rate_limiter import RateLimiter, TimedCache

logger = logging.getLogger(__name__)

RUNN_BASE_URL = os.getenv("RUNN_BASE_URL", "https://api.runn.io")
RUNN_ACCEPT_VERSION = os.getenv("RUNN_ACCEPT_VERSION", "1.0.0")
RUNN_API_TOKEN = os.getenv("RUNN_API_TOKEN", "")

# Cache para roles
_ROLES_CACHE: Optional[List[Dict[str, Any]]] = None

# Rate limiter: 100 requests por minuto (ajustar según límites de Runn)
_RATE_LIMITER = RateLimiter(max_requests=100, window_seconds=60)

# Cache de personas: 5 minutos de TTL
_PEOPLE_CACHE = TimedCache(ttl_seconds=300)


def _extract_people_list(data: Any) -> List[Dict[str, Any]]:
    """Normaliza la respuesta del endpoint /people."""
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]

    if isinstance(data, dict):
        values = data.get("values")
        if isinstance(values, list):
            return [item for item in values if isinstance(item, dict)]

    return []


def _runn_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {RUNN_API_TOKEN}",
        "Accept-Version": RUNN_ACCEPT_VERSION,
        "Content-Type": "application/json",
    }


def runn_get_people() -> List[Dict[str, Any]]:
    """Obtiene todas las personas de Runn utilizando paginación."""
    url = f"{RUNN_BASE_URL}/people/"
    headers = _runn_headers()
    people: List[Dict[str, Any]] = []
    cursor: Optional[str] = None

    while True:
        _RATE_LIMITER.wait_if_needed()
        params = {"cursor": cursor} if cursor else None

        resp = requests.get(url, headers=headers, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        people.extend(_extract_people_list(data))

        if isinstance(data, dict):
            cursor = data.get("nextCursor") or data.get("next_cursor")
        else:
            cursor = None

        if not cursor:
            break

    return people


def runn_find_person_by_email(email: str, use_cache: bool = True) -> Optional[Dict[str, Any]]:
    """
    Búsqueda por email (case-insensitive) con caché.

    Args:
        email: Email de la persona
        use_cache: Si usar caché (default: True)

    Returns:
        Persona encontrada o None
    """
    if not email:
        return None

    email_low = email.strip().lower()

    # Intentar del caché primero
    if use_cache:
        cached_person = _PEOPLE_CACHE.get(email_low)
        if cached_person is not None:
            return cached_person

    person = _fetch_person_by_email(email.strip())
    if person:
        if use_cache:
            _PEOPLE_CACHE.set(email_low, person)
        return person

    if not use_cache:
        return None

    # Fallback: recargar todo el listado (puede ser costoso pero asegura consistencia)
    try:
        for p in runn_get_people():
            em = (p.get("email") or "").strip().lower()
            if em and em == email_low:
                _PEOPLE_CACHE.set(email_low, p)
                return p
    except Exception:
        logger.exception("Failed to refresh people cache from Runn")

    return None


def _fetch_person_by_email(email: str) -> Optional[Dict[str, Any]]:
    if not email:
        return None

    url = f"{RUNN_BASE_URL}/people/"
    params = {"email": email}

    try:
        _RATE_LIMITER.wait_if_needed()
        resp = requests.get(url, headers=_runn_headers(), params=params, timeout=60)
        if not resp.ok:
            logger.error(
                "Failed to fetch person by email from Runn", extra={"email": email, "status": resp.status_code}
            )
            return None

        for person in _extract_people_list(resp.json()):
            em = (person.get("email") or "").strip().lower()
            if em == email.strip().lower():
                return person

        return None
    except Exception:
        logger.exception("Exception fetching person by email from Runn", extra={"email": email})
        return None


def runn_get_roles() -> List[Dict[str, Any]]:
    """
    GET /roles
    Obtiene la lista de roles disponibles.
    Cachea el resultado.
    """
    global _ROLES_CACHE
    
    if _ROLES_CACHE is not None:
        return _ROLES_CACHE
    
    url = f"{RUNN_BASE_URL}/roles"
    try:
        resp = requests.get(url, headers=_runn_headers(), timeout=60)
        resp.raise_for_status()
        data = resp.json()
        _ROLES_CACHE = data if isinstance(data, list) else []
        return _ROLES_CACHE
    except Exception as e:
        logger.exception(f"Failed to fetch roles: {e}")
        return []


def runn_get_role_id_by_name(role_name: str) -> Optional[int]:
    """
    Obtiene el role_id dado un nombre de rol.
    Roles comunes: "employee", "contractor", "placeholder"
    """
    roles = runn_get_roles()
    role_lower = role_name.lower()
    
    for role in roles:
        name = (role.get("name") or "").lower()
        if name == role_lower:
            return role.get("id")
    
    # Fallback: retornar el primer role_id disponible (usualmente "employee")
    if roles:
        return roles[0].get("id")
    
    return None


def runn_upsert_person(
    name: str,
    email: str,
    employment_type: str = "employee",
    starts_at: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Crea o actualiza una persona en Runn v1.0.
    POST /people
    
    IMPORTANTE: En v1.0, se usa role_id (no role_name).
    La creación de persona también crea un contrato automáticamente.
    """
    # Verificar si ya existe
    existing = runn_find_person_by_email(email)
    if existing:
        logger.info(f"Person already exists in Runn: {email}")
        return existing
    
    # Obtener role_id
    role_id = runn_get_role_id_by_name(employment_type)
    if not role_id:
        logger.error(f"Could not find role_id for employment_type: {employment_type}")
        return None
    
    url = f"{RUNN_BASE_URL}/people"
    payload = {
        "name": name or email,
        "email": email,
        "roleId": role_id,  # v1.0 usa roleId, no role_name
    }
    
    # startsAt es opcional
    if starts_at:
        payload["startsAt"] = starts_at
    
    try:
        resp = requests.post(url, headers=_runn_headers(), json=payload, timeout=60)
        if resp.status_code in (200, 201):
            logger.info(f"Person created in Runn: {email}")
            return resp.json()
        
        logger.error(f"runn_upsert_person failed {resp.status_code}: {resp.text}")
        return None
    except Exception as e:
        logger.exception(f"runn_upsert_person exception: {e}")
        return None


def runn_map_category_to_endpoint(category: str) -> str:
    """
    Mapea una categoría a un endpoint de time-off en v1.0.
    
    v1.0 tiene tres tipos:
    - /time-offs/leave (PTO, vacation, sick leave, etc.)
    - /time-offs/holidays (public holidays)
    - /time-offs/rostered-off (RDOs, lieu days)
    """
    category_lower = category.lower()
    
    if "holiday" in category_lower or "public" in category_lower:
        return "holidays"
    
    if "roster" in category_lower or "rostered" in category_lower or "lieu" in category_lower:
        return "rostered-off"
    
    # Default: leave
    return "leave"


def runn_get_existing_leave(
    person_id: int,
    start_date: str,
    end_date: str,
) -> Optional[Dict[str, Any]]:
    """
    Verifica si ya existe un time-off de tipo "leave" para esta persona en estas fechas.
    GET /time-offs/leave?personId={personId}
    
    Esto ayuda con la idempotencia.
    """
    url = f"{RUNN_BASE_URL}/time-offs/leave"
    params = {"personId": person_id}
    
    try:
        resp = requests.get(url, headers=_runn_headers(), params=params, timeout=60)
        if not resp.ok:
            return None
        
        time_offs = resp.json()
        if not isinstance(time_offs, list):
            return None
        
        # Buscar coincidencia de fechas
        for to in time_offs:
            if (to.get("startDate") == start_date and 
                to.get("endDate") == end_date):
                return to
        
        return None
    except Exception as e:
        logger.exception(f"Failed to check existing leave: {e}")
        return None


def runn_create_timeoff(
    *,
    person_id: int,
    start_date: str,
    end_date: Optional[str] = None,
    category: str = "leave",
    note: Optional[str] = None,
    reason: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    POST /time-offs/{type}

    Crea un registro de time-off en Runn v1.0.

    Tipos disponibles:
    - leave: POST /time-offs/leave
    - holidays: POST /time-offs/holidays
    - rostered-off: POST /time-offs/rostered-off

    IMPORTANTE: La API v1.0 hace merge automático de periodos que se traslapan.

    Payload esperado:
    {
        "personId": <int>,
        "startDate": "YYYY-MM-DD",
        "endDate": "YYYY-MM-DD",
        "note": "string" (opcional)
    }

    Returns:
        Objeto del time-off creado con id, personId, startDate, endDate, etc. o None si falla
    """
    _RATE_LIMITER.wait_if_needed()

    # Determinar el endpoint correcto
    endpoint_type = runn_map_category_to_endpoint(category)
    url = f"{RUNN_BASE_URL}/time-offs/{endpoint_type}"

    payload: Dict[str, Any] = {
        "personId": person_id,
        "startDate": start_date,
        "endDate": end_date or start_date,
    }

    if note:
        payload["note"] = note

    try:
        resp = requests.post(url, headers=_runn_headers(), json=payload, timeout=60)
        if resp.status_code in (200, 201):
            result = resp.json()
            logger.info(
                f"Time-off created for person {person_id}: {start_date} to {end_date} "
                f"(type: {endpoint_type}, id: {result.get('id')})"
            )
            return result

        # Log detallado del error
        logger.error(
            f"runn_create_timeoff failed [{resp.status_code}] {url}\n"
            f"Payload: {payload}\n"
            f"Response: {resp.text}"
        )
        return None
    except Exception as e:
        logger.exception(f"runn_create_timeoff exception: {e}")
        return None


def runn_list_person_timeoffs(person_id: int, timeoff_type: str = "leave") -> List[Dict[str, Any]]:
    """
    GET /people/{id}/time-offs/{type}

    Lista los time-offs de una persona por tipo.
    Útil para verificar qué existe antes de crear.

    Tipos: "leave", "holidays", "rostered-off"
    """
    _RATE_LIMITER.wait_if_needed()

    url = f"{RUNN_BASE_URL}/people/{person_id}/time-offs/{timeoff_type}"

    try:
        resp = requests.get(url, headers=_runn_headers(), timeout=60)
        if not resp.ok:
            return []

        data = resp.json()
        return data if isinstance(data, list) else []
    except Exception as e:
        logger.exception(f"Failed to list person time-offs: {e}")
        return []


def runn_update_timeoff(
    *,
    timeoff_id: int,
    category: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    note: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    PUT /time-offs/{type}/{id}

    Actualiza un time-off existente en Runn.

    Args:
        timeoff_id: ID del time-off en Runn
        category: Categoría (leave, holidays, rostered-off)
        start_date: Nueva fecha de inicio (opcional)
        end_date: Nueva fecha de fin (opcional)
        note: Nueva nota (opcional)

    Returns:
        Time-off actualizado o None si falla
    """
    _RATE_LIMITER.wait_if_needed()

    endpoint_type = runn_map_category_to_endpoint(category)
    url = f"{RUNN_BASE_URL}/time-offs/{endpoint_type}/{timeoff_id}"

    payload: Dict[str, Any] = {}

    if start_date:
        payload["startDate"] = start_date
    if end_date:
        payload["endDate"] = end_date
    if note is not None:  # Permitir nota vacía
        payload["note"] = note

    if not payload:
        logger.warning(f"runn_update_timeoff: no changes provided for {timeoff_id}")
        return None

    try:
        resp = requests.put(url, headers=_runn_headers(), json=payload, timeout=60)
        if resp.status_code in (200, 201):
            result = resp.json()
            logger.info(f"Time-off updated: {timeoff_id} (type: {endpoint_type})")
            return result

        logger.error(
            f"runn_update_timeoff failed [{resp.status_code}] {url}\n"
            f"Payload: {payload}\n"
            f"Response: {resp.text}"
        )
        return None
    except Exception as e:
        logger.exception(f"runn_update_timeoff exception: {e}")
        return None


def runn_delete_timeoff(timeoff_id: int, category: str) -> bool:
    """
    DELETE /time-offs/{type}/{id}

    Elimina un time-off de Runn.

    Args:
        timeoff_id: ID del time-off en Runn
        category: Categoría (leave, holidays, rostered-off)

    Returns:
        True si se eliminó exitosamente, False en caso contrario
    """
    _RATE_LIMITER.wait_if_needed()

    endpoint_type = runn_map_category_to_endpoint(category)
    url = f"{RUNN_BASE_URL}/time-offs/{endpoint_type}/{timeoff_id}"

    try:
        resp = requests.delete(url, headers=_runn_headers(), timeout=60)
        if resp.status_code in (200, 204):
            logger.info(f"Time-off deleted: {timeoff_id} (type: {endpoint_type})")
            return True

        logger.error(
            f"runn_delete_timeoff failed [{resp.status_code}] {url}\n"
            f"Response: {resp.text}"
        )
        return False
    except Exception as e:
        logger.exception(f"runn_delete_timeoff exception: {e}")
        return False


def runn_clear_people_cache() -> None:
    """
    Limpia el caché de personas.
    Útil para forzar recarga después de cambios importantes.
    """
    _PEOPLE_CACHE.clear()
    logger.info("Runn people cache cleared")


def runn_get_person_contracts(person_id: int) -> List[Dict[str, Any]]:
    """
    GET /people/{id}/contracts

    Obtiene todos los contratos de una persona en Runn.
    En Runn, cada persona puede tener múltiples contratos
    (e.g., diferentes periodos, cambios de rol).

    Args:
        person_id: ID de la persona en Runn

    Returns:
        Lista de contratos con id, startDate, endDate, roleId, costPerHour, etc.
    """
    _RATE_LIMITER.wait_if_needed()

    url = f"{RUNN_BASE_URL}/people/{person_id}/contracts"

    try:
        resp = requests.get(url, headers=_runn_headers(), timeout=60)
        if not resp.ok:
            logger.error(
                f"Failed to fetch contracts for person {person_id}: {resp.status_code}"
            )
            return []

        data = resp.json()

        # Normalizar respuesta: puede ser lista directa o {"values": [...]}
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]

        if isinstance(data, dict):
            values = data.get("values")
            if isinstance(values, list):
                return [item for item in values if isinstance(item, dict)]

        return []
    except Exception as e:
        logger.exception(f"Exception fetching contracts for person {person_id}: {e}")
        return []


def runn_get_active_contracts(
    person_id: int,
    reference_date: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Obtiene contratos activos de una persona en una fecha específica.

    Un contrato es activo si:
    - startDate <= reference_date (o hoy)
    - endDate >= reference_date (o hoy) O endDate is None

    Args:
        person_id: ID de persona en Runn
        reference_date: Fecha de referencia YYYY-MM-DD (default: hoy)

    Returns:
        Lista de contratos activos
    """
    import datetime as dt

    contracts = runn_get_person_contracts(person_id)

    if not contracts:
        return []

    # Fecha de referencia (hoy si no se especifica)
    if reference_date:
        ref = reference_date
    else:
        ref = dt.date.today().isoformat()

    active = []
    for contract in contracts:
        start = contract.get("startDate", "")
        end = contract.get("endDate")

        # Debe haber comenzado antes o en la fecha de referencia
        if start > ref:
            continue

        # Si tiene endDate, debe ser posterior o igual a la fecha de referencia
        # Si no tiene endDate, está abierto (activo)
        if end is None or end >= ref:
            active.append(contract)

    return active


def runn_update_contract_cost(
    contract_id: int,
    cost_per_hour: float
) -> Optional[Dict[str, Any]]:
    """
    PATCH /contracts/{id}

    Actualiza el costPerHour de un contrato en Runn v1.0.

    Args:
        contract_id: ID del contrato en Runn
        cost_per_hour: Costo por hora (calculado desde ChartHop)

    Returns:
        Contrato actualizado o None si falla
    """
    _RATE_LIMITER.wait_if_needed()

    url = f"{RUNN_BASE_URL}/contracts/{contract_id}"

    # Redondear a 2 decimales para evitar diferencias de precisión
    cost_rounded = round(cost_per_hour, 2)

    payload = {
        "costPerHour": cost_rounded
    }

    try:
        resp = requests.patch(url, headers=_runn_headers(), json=payload, timeout=60)
        if resp.status_code in (200, 201):
            result = resp.json()
            logger.info(
                f"Contract {contract_id} cost updated to {cost_rounded}/hour"
            )
            return result

        logger.error(
            f"runn_update_contract_cost failed [{resp.status_code}] {url}\n"
            f"Payload: {payload}\n"
            f"Response: {resp.text}"
        )
        return None
    except Exception as e:
        logger.exception(f"runn_update_contract_cost exception: {e}")
        return None
