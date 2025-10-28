from __future__ import annotations

import os
import logging
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

RUNN_BASE_URL = os.getenv("RUNN_BASE_URL", "https://api.runn.io")
RUNN_ACCEPT_VERSION = os.getenv("RUNN_ACCEPT_VERSION", "1.0.0")
RUNN_API_TOKEN = os.getenv("RUNN_API_TOKEN", "")

# Cache para time-off types
_TIME_OFF_TYPES_CACHE: Optional[List[Dict[str, Any]]] = None


def _runn_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {RUNN_API_TOKEN}",
        "Accept-Version": RUNN_ACCEPT_VERSION,
        "Content-Type": "application/json",
    }


def runn_get_people() -> List[Dict[str, Any]]:
    """
    GET /people (v1)
    """
    url = f"{RUNN_BASE_URL}/people"
    headers = _runn_headers()
    resp = requests.get(url, headers=headers, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else []


def runn_find_person_by_email(email: str) -> Optional[Dict[str, Any]]:
    """
    Búsqueda simple por email (case-insensitive) en la lista de people.
    """
    if not email:
        return None
    email_low = email.strip().lower()
    for p in runn_get_people():
        em = (p.get("email") or "").strip().lower()
        if em and em == email_low:
            return p
    return None


def runn_upsert_person(
    name: str,
    email: str,
    employment_type: str = "employee",
    starts_at: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Crea o actualiza una persona en Runn.
    POST /people
    """
    # Primero verifica si ya existe
    existing = runn_find_person_by_email(email)
    if existing:
        # Podrías implementar un PATCH aquí si necesitas actualizar
        logger.info(f"Person already exists in Runn: {email}")
        return existing
    
    url = f"{RUNN_BASE_URL}/people"
    payload = {
        "name": name or email,
        "email": email,
        "role": employment_type,  # 'employee', 'contractor', etc.
    }
    if starts_at:
        payload["startsAt"] = starts_at
    
    try:
        resp = requests.post(url, headers=_runn_headers(), json=payload, timeout=60)
        if resp.status_code in (200, 201):
            return resp.json()
        logger.error(f"runn_upsert_person failed {resp.status_code}: {resp.text}")
        return None
    except Exception as e:
        logger.exception(f"runn_upsert_person exception: {e}")
        return None


def runn_get_time_off_types() -> List[Dict[str, Any]]:
    """
    GET /time-off-types
    Devuelve los tipos de time-off disponibles en la cuenta de Runn.
    Cachea el resultado ya que estos tipos no cambian frecuentemente.
    """
    global _TIME_OFF_TYPES_CACHE
    
    if _TIME_OFF_TYPES_CACHE is not None:
        return _TIME_OFF_TYPES_CACHE
    
    url = f"{RUNN_BASE_URL}/time-off-types"
    try:
        resp = requests.get(url, headers=_runn_headers(), timeout=60)
        resp.raise_for_status()
        data = resp.json()
        _TIME_OFF_TYPES_CACHE = data if isinstance(data, list) else []
        return _TIME_OFF_TYPES_CACHE
    except Exception as e:
        logger.exception(f"Failed to fetch time-off types: {e}")
        return []


def runn_map_category_to_type_id(category: str, reason: Optional[str] = None) -> Optional[int]:
    """
    Mapea una categoría de ChartHop a un timeOffTypeId de Runn.
    
    Estrategia:
    1. Busca por nombre exacto en los tipos disponibles
    2. Si no encuentra, usa el primer tipo disponible como fallback
    3. Si hay un 'reason', intenta matchear por nombre
    """
    types = runn_get_time_off_types()
    if not types:
        logger.warning("No time-off types available in Runn")
        return None
    
    category_lower = category.lower()
    reason_lower = (reason or "").lower()
    
    # Mapeo de categorías comunes
    category_map = {
        "leave": ["annual leave", "vacation", "pto", "leave"],
        "holidays": ["public holiday", "holiday", "bank holiday"],
        "rostered-off": ["rostered day off", "rdo", "lieu"],
        "sick": ["sick leave", "sick"],
    }
    
    # Intenta match por razón primero
    if reason_lower:
        for ttype in types:
            type_name = (ttype.get("name") or "").lower()
            if reason_lower in type_name or type_name in reason_lower:
                return ttype.get("id")
    
    # Luego por categoría
    search_terms = category_map.get(category_lower, [category_lower])
    for ttype in types:
        type_name = (ttype.get("name") or "").lower()
        for term in search_terms:
            if term in type_name:
                return ttype.get("id")
    
    # Fallback: primer tipo disponible
    logger.warning(f"No match for category '{category}', using first available type")
    return types[0].get("id")


def runn_create_timeoff(
    *,
    person_id: int,
    start_date: str,
    end_date: Optional[str] = None,
    category: str = "leave",
    note: Optional[str] = None,
    reason: Optional[str] = None,
) -> bool:
    """
    POST /people/{personId}/time-off
    
    Crea un registro de time-off en Runn.
    
    Payload esperado:
    {
        "startDate": "YYYY-MM-DD",
        "endDate": "YYYY-MM-DD",
        "timeOffTypeId": <int>,  # REQUERIDO
        "note": "string"
    }
    """
    # Obtener el timeOffTypeId apropiado
    time_off_type_id = runn_map_category_to_type_id(category, reason)
    if not time_off_type_id:
        logger.error(f"Cannot create time-off: no valid timeOffTypeId for category '{category}'")
        return False
    
    url = f"{RUNN_BASE_URL}/people/{person_id}/time-off"
    payload: Dict[str, Any] = {
        "startDate": start_date,
        "endDate": end_date or start_date,
        "timeOffTypeId": time_off_type_id,
    }
    if note:
        payload["note"] = note
    
    try:
        resp = requests.post(url, headers=_runn_headers(), json=payload, timeout=60)
        if resp.status_code in (200, 201):
            logger.info(f"Time-off created for person {person_id}: {start_date} to {end_date}")
            return True
        
        # Log detallado del error
        logger.error(
            f"runn_create_timeoff failed [{resp.status_code}] {url}\n"
            f"Payload: {payload}\n"
            f"Response: {resp.text}"
        )
        return False
    except Exception as e:
        logger.exception(f"runn_create_timeoff exception: {e}")
        return False


def runn_get_existing_timeoff(
    person_id: int,
    start_date: str,
    end_date: str,
) -> Optional[Dict[str, Any]]:
    """
    Verifica si ya existe un time-off para esta persona en estas fechas.
    GET /people/{personId}/time-off
    
    Esto ayuda con la idempotencia.
    """
    url = f"{RUNN_BASE_URL}/people/{person_id}/time-off"
    try:
        resp = requests.get(url, headers=_runn_headers(), timeout=60)
        if not resp.ok:
            return None
        
        time_offs = resp.json()
        if not isinstance(time_offs, list):
            return None
        
        # Busca coincidencia exacta de fechas
        for to in time_offs:
            if (to.get("startDate") == start_date and 
                to.get("endDate") == end_date):
                return to
        
        return None
    except Exception as e:
        logger.exception(f"Failed to check existing time-off: {e}")
        return None
