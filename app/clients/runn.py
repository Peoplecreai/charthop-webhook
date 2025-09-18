from __future__ import annotations

from typing import Optional

import requests

from app.utils.config import HTTP_TIMEOUT, RUNN_API, runn_headers


def runn_find_person_by_email(email: str) -> Optional[dict]:
    if not email:
        return None
    try:
        resp = requests.get(
            f"{RUNN_API}/people",
            headers=runn_headers(),
            params={"email": email},
            timeout=HTTP_TIMEOUT,
        )
        if resp.ok and isinstance(resp.json(), list) and resp.json():
            return (resp.json() or [None])[0]
    except Exception as exc:  # pragma: no cover - logging
        print("runn_find_person_by_email error:", repr(exc))
    return None


def runn_upsert_person(
    *,
    name: str,
    email: Optional[str],
    role_id: Optional[str] = None,
    team_id: Optional[str] = None,
    employment_type: Optional[str] = None,
    starts_at: Optional[str] = None,
) -> Optional[dict]:
    payload: dict = {"name": name}
    if email:
        payload["email"] = email
    if role_id:
        payload["role_id"] = role_id
    if team_id:
        payload["team_id"] = team_id
    if employment_type:
        payload["employment_type"] = employment_type
    if starts_at:
        payload["starts_at"] = starts_at

    try:
        resp = requests.post(
            f"{RUNN_API}/people",
            headers=runn_headers(),
            json=payload,
            timeout=HTTP_TIMEOUT,
        )
        if resp.status_code in (200, 201):
            return resp.json()
        if resp.status_code == 409 and email:
            existing = runn_find_person_by_email(email)
            if existing and existing.get("id"):
                person_id = existing["id"]
                upd = requests.patch(
                    f"{RUNN_API}/people/{person_id}",
                    headers=runn_headers(),
                    json=payload,
                    timeout=HTTP_TIMEOUT,
                )
                if upd.ok:
                    return {"id": person_id}
        resp.raise_for_status()
    except Exception as exc:  # pragma: no cover - logging
        print("runn_upsert_person error:", repr(exc))
    return None


def runn_create_leave(
    *,
    person_id: str,
    starts_at: str,
    ends_at: str,
    reason: str = "Vacation",
    external_ref: Optional[str] = None,
) -> Optional[dict]:
    payload = {"personId": person_id, "startsAt": starts_at, "endsAt": ends_at, "reason": reason}
    if external_ref:
        payload["externalRef"] = external_ref
    try:
        resp = requests.post(
            f"{RUNN_API}/time-offs/leave",
            headers=runn_headers(),
            json=payload,
            timeout=HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:  # pragma: no cover - logging
        print("runn_create_leave error:", repr(exc))
    return None
