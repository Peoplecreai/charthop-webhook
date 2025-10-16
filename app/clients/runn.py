from __future__ import annotations

import os
import logging
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

RUNN_BASE_URL = os.getenv("RUNN_BASE_URL", "https://api.runn.io")
RUNN_ACCEPT_VERSION = os.getenv("RUNN_ACCEPT_VERSION", "1.0.0")
RUNN_API_TOKEN = os.getenv("RUNN_API_TOKEN", "")

def _runn_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {RUNN_API_TOKEN}",
        "Accept-Version": RUNN_ACCEPT_VERSION,
        "Content-Type": "application/json",
    }

def runn_get_people() -> List[Dict[str, Any]]:
    """
    GET /people/  (v1)
    """
    url = f"{RUNN_BASE_URL}/people/"
    headers = _runn_headers()
    resp = requests.get(url, headers=headers, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    # La API de Runn v1 devuelve una lista directamente
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
    Ejemplo de upsert simple de persona (si ya lo tienes implementado en otro lugar,
    puedes mantener tu versión). Aquí ilustro un POST trivial.
    """
    url = f"{RUNN_BASE_URL}/people/"
    payload = {
        "name": name or email,
        "email": email,
        "employmentType": employment_type,
    }
    if starts_at:
        payload["startsAt"] = starts_at  # string "YYYY-MM-DD"
    try:
        resp = requests.post(url, headers=_runn_headers(), json=payload, timeout=60)
        if resp.status_code in (200, 201):
            return resp.json()
        logger.error("runn_upsert_person failed %s %s", resp.status_code, resp.text)
        return None
    except Exception as e:
        logger.exception("runn_upsert_person exception: %s", e)
        return None

def runn_create_timeoff(
    *,
    person_id: int,
    start_date: str,
    end_date: Optional[str] = None,
    minutes_per_day: int = 480,
    note: Optional[str] = None,
    category: str = "leave",  # "leave" | "holidays" | "rostered-off"
) -> bool:
    """
    POST /time-offs/{category}/  (v1)
    Payload v1:
      {
        "personId": <int>,
        "startDate": "YYYY-MM-DD",
        "endDate":   "YYYY-MM-DD",
        "minutesPerDay": <int>,
        "note": "string"
      }
    """
    if category not in ("leave", "holidays", "rostered-off"):
        category = "leave"

    url = f"{RUNN_BASE_URL}/time-offs/{category}/"
    payload: Dict[str, Any] = {
        "personId": person_id,
        "startDate": start_date,
        "endDate": end_date or start_date,
        "minutesPerDay": int(minutes_per_day),
    }
    if note:
        payload["note"] = note

    try:
        resp = requests.post(url, headers=_runn_headers(), json=payload, timeout=60)
        if resp.status_code in (200, 201):
            return True
        logger.error(
            "runn_create_timeoff failed [%s] %s - %s",
            resp.status_code, url, resp.text
        )
        return False
    except Exception as e:
        logger.exception("runn_create_timeoff exception: %s", e)
        return False
