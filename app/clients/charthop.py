from __future__ import annotations

import csv
import io
import json
import hashlib
import os
import time
import datetime as dt
from collections import OrderedDict
from typing import Any, Dict, Iterable, Iterator, List, Optional

from requests import Session
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import HTTPError, Timeout, SSLError

from app.utils import config as _config

AUTO_ASSIGN_WORK_EMAIL = _config.AUTO_ASSIGN_WORK_EMAIL
CH_API = _config.CH_API
CH_ORG_ID = _config.CH_ORG_ID
CH_PEOPLE_PAGE_SIZE = getattr(_config, "CH_PEOPLE_PAGE_SIZE", 200)
CORP_EMAIL_DOMAIN = _config.CORP_EMAIL_DOMAIN
HTTP_TIMEOUT = _config.HTTP_TIMEOUT
ch_headers = _config.ch_headers
strip_accents_and_non_alnum = _config.strip_accents_and_non_alnum

# =========================
#   HTTP helpers
# =========================

def _new_session() -> Session:
    s = Session()
    s.headers.update(ch_headers())
    adapter = HTTPAdapter(pool_connections=4, pool_maxsize=8, max_retries=0)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def _get_json(session: Session, url: str, params: Dict[str, str], max_retries: int = 5) -> Dict:
    attempt = 0
    last_exc: Optional[Exception] = None
    while True:
        try:
            r = session.get(url, params=params, timeout=HTTP_TIMEOUT)
        except (RequestsConnectionError, Timeout, SSLError) as exc:
            attempt += 1
            last_exc = exc
        else:
            if r.status_code == 429:
                attempt += 1
                last_exc = HTTPError("429 Too Many Requests", response=r)
            else:
                try:
                    r.raise_for_status()
                except HTTPError as exc:
                    attempt += 1
                    last_exc = exc
                else:
                    try:
                        return r.json() or {}
                    except ValueError as exc:
                        attempt += 1
                        last_exc = exc
        if attempt > max_retries:
            raise RuntimeError(f"ChartHop request failed after retries: {last_exc}") from last_exc
        time.sleep(min(2 ** (attempt - 1), 30))


def ch_get_paginated(url: str, params: Dict[str, object]) -> List[Dict]:
    session = _new_session()
    results: List[Dict] = []
    offset: Optional[str] = None
    try:
        while True:
            query: Dict[str, object] = {}
            for key, value in params.items():
                if value is None:
                    continue
                if isinstance(value, (list, tuple)):
                    query[str(key)] = value
                else:
                    query[str(key)] = str(value)
            if offset:
                query["offset"] = offset
            payload = _get_json(session, url, query)
            data = payload.get("data") if isinstance(payload, dict) else None
            if data is None:
                data = payload
            if isinstance(data, dict):
                data = [data]
            if not data:
                break
            results.extend(data)
            next_token = payload.get("next") if isinstance(payload, dict) else None
            if not next_token:
                break
            offset = str(next_token)
    finally:
        session.close()
    return results


# =========================
#   People (v2) + cursor
# =========================

# Campos proyectados desde /v2/org/{org}/person (con rutas de punto)
PEOPLE_FIELDS = ",".join(
    [
        "id",                          # CH person id (para lookups)
        "contact.employee",            # Employee Id preferido
        "jobId",                       # para Employment Type
        "contact.workEmail",
        "manager.contact.workEmail",
        "name.first",
        "name.last",
        "name.pref",
        "name.preflast",
        "address.city",
        "address.country",
        "title",
        "seniority",
        "startDateOrg",
        "endDateOrg",
        "department.name",
        "gender",
    ]
)

def ch_iter_people_v2(fields: str = PEOPLE_FIELDS, page_size: Optional[int] = None) -> Iterator[Dict]:
    """
    Itera personas 'vigentes' (includeAll=false) paginando con cursor `next`.
    Devuelve dicts con claves "aplanadas" (ChartHop v2 retorna keys con punto).
    """
    url = f"{CH_API}/v2/org/{CH_ORG_ID}/person"
    session = _new_session()
    limit = page_size or CH_PEOPLE_PAGE_SIZE or 200
    if limit <= 0:
        limit = 200

    cursor: Optional[str] = None
    seen_cursors: set[str] = set()

    try:
        while True:
            params = {
                "fields": fields,
                "limit": limit,
                "includeAll": False,
            }
            if cursor:
                if cursor in seen_cursors:
                    break
                # ChartHop v2 person listing (see /v2/org/{orgId}/person in the swagger)
                # uses the `from` query parameter to continue pagination.
                params["from"] = cursor

            payload = _get_json(session, url, params)
            data = payload.get("data") or []
            if isinstance(data, dict):
                data = [data]
            if not data:
                break

            for item in data:
                yield item

            next_token = payload.get("next")
            if not next_token:
                break
            seen_cursors.add(cursor or "")
            cursor = str(next_token)
    finally:
        session.close()


# =========================
#   Job lookup (employment)
# =========================

def ch_get_job_employment(job_id: str, session: Optional[Session] = None) -> Optional[str]:
    if not job_id:
        return None
    own = False
    if session is None:
        session = _new_session()
        own = True
    try:
        url = f"{CH_API}/v2/org/{CH_ORG_ID}/job/{job_id}"
        payload = _get_json(session, url, {"fields": "employment"})
        return (payload or {}).get("employment") or None
    finally:
        if own:
            session.close()


PEOPLE_COMPENSATION_FIELDS = ",".join([
    "id",
    "contact.workEmail",
    "contact.personalEmail",
    "name.first",
    "name.last",
    "name.full",
    "comp.costtocompany",
    "comp.currency",
    "employmentType",
    "employment",
])


def ch_get_person_compensation(person_id: str) -> Optional[Dict[str, Any]]:
    """
    GET /v2/org/{orgId}/person/{personId}

    Obtiene información de compensación de una persona.

    Args:
        person_id: ID de la persona en ChartHop

    Returns:
        {
            "person_id": "abc123",
            "email": "user@example.com",
            "name": "John Doe",
            "cost_to_company": 100000.0,
            "currency": "USD",
            "employment_type": "employee"
        }
        o None si no se encuentra o falla
    """
    person_id = (person_id or "").strip()
    if not person_id:
        return None

    session = _new_session()
    try:
        url = f"{CH_API}/v2/org/{CH_ORG_ID}/person/{person_id}"
        payload = _get_json(
            session,
            url,
            {"fields": PEOPLE_COMPENSATION_FIELDS}
        )

        if not payload:
            return None

        # Extraer email (preferir work email)
        work_email = (payload.get("contact.workEmail") or "").strip()
        personal_email = (payload.get("contact.personalEmail") or "").strip()
        email = work_email or personal_email

        if not email:
            return None

        # Extraer nombre
        name_full = (payload.get("name.full") or "").strip()
        if not name_full:
            first = (payload.get("name.first") or "").strip()
            last = (payload.get("name.last") or "").strip()
            name_full = f"{first} {last}".strip()

        # Extraer compensación
        cost_to_company = payload.get("comp.costtocompany")
        if cost_to_company is not None:
            try:
                cost_to_company = float(cost_to_company)
            except (ValueError, TypeError):
                cost_to_company = None

        currency = (payload.get("comp.currency") or "USD").strip()

        # Employment type
        employment_type = (
            payload.get("employmentType") or
            payload.get("employment") or
            "employee"
        ).strip()

        return {
            "person_id": person_id,
            "email": email,
            "name": name_full,
            "cost_to_company": cost_to_company,
            "currency": currency,
            "employment_type": employment_type,
        }

    except HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            return None
        raise
    finally:
        session.close()


def ch_fetch_people_with_compensation(
    active_only: bool = True
) -> List[Dict[str, Any]]:
    """
    Itera sobre todas las personas y extrae información de compensación.

    Args:
        active_only: Solo personas activas (default: True)

    Returns:
        Lista de personas con compensación:
        [
            {
                "person_id": "abc123",
                "email": "user@example.com",
                "name": "John Doe",
                "cost_to_company": 100000.0,
                "currency": "USD",
                "employment_type": "employee"
            },
            ...
        ]
    """
    results: List[Dict[str, Any]] = []

    for person in ch_iter_people_v2(PEOPLE_COMPENSATION_FIELDS):
        person_id = (person.get("id") or "").strip()
        if not person_id:
            continue

        # Email
        work_email = (person.get("contact.workEmail") or "").strip()
        personal_email = (person.get("contact.personalEmail") or "").strip()
        email = work_email or personal_email

        if not email:
            continue

        # Nombre
        name_full = (person.get("name.full") or "").strip()
        if not name_full:
            first = (person.get("name.first") or "").strip()
            last = (person.get("name.last") or "").strip()
            name_full = f"{first} {last}".strip()

        # Compensación
        cost_to_company = person.get("comp.costtocompany")
        if cost_to_company is not None:
            try:
                cost_to_company = float(cost_to_company)
            except (ValueError, TypeError):
                cost_to_company = None

        # Skip si no hay compensación y estamos filtrando
        if active_only and cost_to_company is None:
            continue

        currency = (person.get("comp.currency") or "USD").strip()

        employment_type = (
            person.get("employmentType") or
            person.get("employment") or
            "employee"
        ).strip()

        results.append({
            "person_id": person_id,
            "email": email,
            "name": name_full,
            "cost_to_company": cost_to_company,
            "currency": currency,
            "employment_type": employment_type,
        })

    return results


# =========================
#   Culture Amp rows
# =========================

CULTURE_AMP_COLUMNS = [
    "Employee Id",
    "Email",
    "Name",
    "Preferred Name",
    "Manager Email",
    "Manager",
    "Location",
    "Job Title",
    "Seniority",
    "Start Date",
    "End Date",
    "Department",
    "Country",
    "Employment Type",
    "Gender",
]

def _norm_date_str(s: Optional[str]) -> str:
    s = (s or "").strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    return s

def _row_hash(row: dict) -> str:
    """
    Hash estable para detectar cambios relevantes en Culture Amp.
    Usa solo el contenido de la fila (orden de claves determinista).
    """
    canonical = json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def iter_culture_amp_rows_with_ids() -> Iterator[tuple[Dict[str, str], str]]:
    """
    Devuelve (row_CA, ch_person_id).
    Employment Type se resuelve consultando Job una vez por jobId (cache local).
    """
    job_cache: Dict[str, Optional[str]] = {}
    session = _new_session()
    try:
        for p in ch_iter_people_v2(PEOPLE_FIELDS):
            email = (p.get("contact.workEmail") or "").strip()
            if not email:
                continue

            emp_id = (
                (p.get("contact.employee") or "").strip()
                or (p.get("id") or "").strip()
                or email
            )
            ch_person_id = (p.get("id") or "").strip()

            pref_first = (p.get("name.pref") or "").strip()
            pref_last = (p.get("name.preflast") or "").strip()
            first = (p.get("name.first") or "").strip()
            last = (p.get("name.last") or "").strip()

            name_first = pref_first or first
            name_last = pref_last or last
            if name_first or name_last:
                name = f"{name_first} {name_last}".strip()
            else:
                name = ""

            manager_email = (p.get("manager.contact.workEmail") or "").strip()
            city = (p.get("address.city") or "").strip()
            country = (p.get("address.country") or "").strip()
            title = (p.get("title") or "").strip()
            seniority = (p.get("seniority") or "").strip()
            start_date = _norm_date_str(p.get("startDateOrg"))
            end_date = _norm_date_str(p.get("endDateOrg"))
            department = (p.get("department.name") or "").strip()
            gender = (p.get("gender") or "").strip()

            job_id = (p.get("jobId") or "").strip()
            if job_id:
                if job_id in job_cache:
                    employment = job_cache[job_id] or ""
                else:
                    employment = ch_get_job_employment(job_id, session=session) or ""
                    job_cache[job_id] = employment
            else:
                employment = ""

            row = {
                "Employee Id": emp_id,
                "Email": email,
                "Name": name,
                "Preferred Name": pref_first,
                "Manager Email": manager_email,
                "Manager": manager_email,
                "Location": city,
                "Job Title": title,
                "Seniority": seniority,
                "Start Date": start_date,
                "End Date": end_date,
                "Department": department,
                "Country": country,
                "Employment Type": employment,
                "Gender": gender,
            }
            yield row, ch_person_id
    finally:
        session.close()


def iter_culture_amp_rows() -> Iterator[Dict[str, str]]:
    for row, _pid in iter_culture_amp_rows_with_ids():
        yield row


def build_culture_amp_rows() -> List[Dict[str, str]]:
    return list(iter_culture_amp_rows())


def culture_amp_csv_from_rows(rows: Iterable[Dict[str, str]]) -> str:
    sio = io.StringIO()
    writer = csv.DictWriter(
        sio,
        fieldnames=CULTURE_AMP_COLUMNS,
        extrasaction="ignore",
        lineterminator="\n",
    )
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return sio.getvalue()


# =========================
#   Shared helpers
# =========================


def _extract_entity(payload: Dict) -> Dict:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict):
            return data
        return payload
    return {}


def _normalize_date_arg(value: Optional[dt.date | dt.datetime]) -> Optional[dt.date]:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return None


def _parse_iso_date(value: Optional[str]) -> Optional[dt.date]:
    if not value:
        return None
    value = str(value).strip()
    if not value:
        return None
    if len(value) >= 10:
        value = value[:10]
    try:
        return dt.date.fromisoformat(value)
    except ValueError:
        return None


def _stringify_fields(data: Dict[str, object]) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for key, value in data.items():
        if isinstance(value, str):
            result[key] = value.strip()
        elif value is None:
            result[key] = ""
        else:
            result[key] = str(value)
    return result


# =========================
#   Job helpers
# =========================


def ch_find_job(job_id: str) -> Optional[Dict]:
    job_id = (job_id or "").strip()
    if not job_id:
        return None
    session = _new_session()
    try:
        url = f"{CH_API}/v2/org/{CH_ORG_ID}/job/{job_id}"
        resp = session.get(url, params={"include": "fields"}, timeout=HTTP_TIMEOUT)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        try:
            payload = resp.json() or {}
        except ValueError:
            return {}
        entity = _extract_entity(payload)
        return entity or payload
    except HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            return None
        raise
    finally:
        session.close()


def ch_upsert_job_field(job_id: str, field_api_name: str, value: object) -> Dict:
    job_id = (job_id or "").strip()
    field_api_name = (field_api_name or "").strip()
    if not job_id:
        raise ValueError("job_id is required")
    if not field_api_name:
        raise ValueError("field_api_name is required")
    session = _new_session()
    try:
        url = f"{CH_API}/v2/org/{CH_ORG_ID}/job/{job_id}"
        payload = {"fields": {field_api_name: value}}
        resp = session.patch(url, json=payload, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        try:
            body = resp.json() or {}
        except ValueError:
            body = {}
        entity = _extract_entity(body)
        return entity or body
    finally:
        session.close()


# =========================
#   Teamtailor hires helpers
# =========================


def _normalize_import_rows(rows: Iterable[Dict[str, object]]) -> tuple[List[OrderedDict[str, str]], List[str]]:
    normalized: List[OrderedDict[str, str]] = []
    fieldnames: List[str] = []
    seen_fields: set[str] = set()
    for row in rows:
        if not row:
            continue
        ordered: OrderedDict[str, str] = OrderedDict()
        for key, value in row.items():
            if key is None:
                continue
            key_str = str(key).strip()
            if not key_str:
                continue
            if key_str not in seen_fields:
                fieldnames.append(key_str)
                seen_fields.add(key_str)
            if isinstance(value, str):
                ordered[key_str] = value.strip()
            elif value is None:
                ordered[key_str] = ""
            else:
                ordered[key_str] = str(value)
        if ordered:
            normalized.append(ordered)
    return normalized, fieldnames


def ch_import_people_csv(rows: Iterable[Dict[str, object]]) -> Dict:
    normalized_rows, fieldnames = _normalize_import_rows(rows)
    if not normalized_rows:
        return {"submitted": False, "reason": "no rows"}

    if not fieldnames:
        fieldnames = list(normalized_rows[0].keys())

    sio = io.StringIO()
    writer = csv.DictWriter(sio, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n")
    writer.writeheader()
    for row in normalized_rows:
        writer.writerow(row)
    csv_payload = sio.getvalue()

    session = _new_session()
    try:
        create_resp = session.post(
            f"{CH_API}/v1/org/{CH_ORG_ID}/import/csv",
            json={"type": "person", "recordType": "person"},
            timeout=HTTP_TIMEOUT,
        )
        create_resp.raise_for_status()
        try:
            create_body = create_resp.json() or {}
        except ValueError:
            create_body = {}
        import_id = (
            create_body.get("importId")
            or create_body.get("import_id")
            or create_body.get("id")
        )
        if not import_id:
            raise RuntimeError("ChartHop CSV import did not return an importId")

        data_resp = session.post(
            f"{CH_API}/v1/org/{CH_ORG_ID}/import/csv/data",
            json={"importId": import_id, "data": csv_payload, "hasHeaders": True},
            timeout=HTTP_TIMEOUT,
        )
        data_resp.raise_for_status()

        submit_resp = session.post(
            f"{CH_API}/v1/org/{CH_ORG_ID}/import/csv/submit",
            json={"importId": import_id, "options": {"sendInviteEmails": False}},
            timeout=HTTP_TIMEOUT,
        )
        submit_resp.raise_for_status()
        try:
            submit_body = submit_resp.json() or {}
        except ValueError:
            submit_body = {}

        result = {
            "importId": import_id,
            "rows": len(normalized_rows),
            "submitted": True,
        }
        if submit_body:
            result["response"] = submit_body
        return result
    finally:
        session.close()


def generate_unique_work_email(first_name: str, last_name: str) -> str:
    if not AUTO_ASSIGN_WORK_EMAIL:
        return ""
    domain = (CORP_EMAIL_DOMAIN or "").strip().lower()
    if not domain:
        return ""
    domain = domain.lstrip("@")
    first_slug = strip_accents_and_non_alnum(first_name)
    last_slug = strip_accents_and_non_alnum(last_name)
    parts = [part for part in (first_slug, last_slug) if part]
    base = ".".join(parts) if parts else "team"
    base = base.strip(".") or "team"

    existing: set[str] = set()
    for person in ch_iter_people_v2("contact.workEmail,contact.personalEmail"):
        work = (person.get("contact.workEmail") or "").strip().lower()
        personal = (person.get("contact.personalEmail") or "").strip().lower()
        if work:
            existing.add(work)
        if personal:
            existing.add(personal)

    candidate = f"{base}@{domain}"
    if candidate not in existing:
        return candidate

    for idx in range(2, 1000):
        candidate = f"{base}{idx}@{domain}"
        if candidate not in existing:
            return candidate

    raise RuntimeError("No hay emails disponibles con el dominio corporativo")


# =========================
#   Runn integrations helpers
# =========================


PEOPLE_ONBOARD_FIELDS = ",".join(
    [
        "id",
        "contact.employee",
        "jobId",
        "employmentType",
        "contact.workEmail",
        "contact.personalEmail",
        "name.first",
        "name.last",
        "name.pref",
        "name.preflast",
        "name.full",
        "manager.contact.workEmail",
        "startDateOrg",
        "endDateOrg",
    ]
)


def ch_people_starting_between(
    start: Optional[dt.date | dt.datetime], end: Optional[dt.date | dt.datetime]
) -> List[Dict]:
    start_date = _normalize_date_arg(start)
    end_date = _normalize_date_arg(end)
    results: List[Dict] = []
    job_cache: Dict[str, Optional[str]] = {}
    job_session = _new_session()
    try:
        for person in ch_iter_people_v2(PEOPLE_ONBOARD_FIELDS):
            start_raw = (person.get("startDateOrg") or "").strip()
            start_dt = _parse_iso_date(start_raw)
            if start_dt is None:
                continue
            if start_date and start_dt < start_date:
                continue
            if end_date and start_dt > end_date:
                continue

            person_id = (person.get("id") or "").strip()
            job_id = (person.get("jobId") or "").strip()
            employment = (person.get("employmentType") or "").strip()
            if not employment and job_id:
                if job_id in job_cache:
                    employment = job_cache[job_id] or ""
                else:
                    employment = ch_get_job_employment(job_id, session=job_session) or ""
                    job_cache[job_id] = employment

            pref_first = (person.get("name.pref") or "").strip()
            pref_last = (person.get("name.preflast") or "").strip()
            legal_first = (person.get("name.first") or "").strip()
            legal_last = (person.get("name.last") or "").strip()
            first_value = pref_first or legal_first
            last_value = pref_last or legal_last
            full_name = (person.get("name.full") or "").strip()
            if not full_name:
                full_name = f"{first_value} {last_value}".strip()

            fields = {
                "employee id": (person.get("contact.employee") or person_id),
                "job id": job_id,
                "name": full_name,
                "name first": first_value,
                "name last": last_value,
                "employment type": employment,
                "employmenttype": employment,
                "start date": _norm_date_str(start_raw),
                "startdate": _norm_date_str(start_raw),
                "end date": _norm_date_str(person.get("endDateOrg")),
                "contact workemail": (person.get("contact.workEmail") or ""),
                "contact work email": (person.get("contact.workEmail") or ""),
                "contact personalemail": (person.get("contact.personalEmail") or ""),
                "manager contact workemail": (person.get("manager.contact.workEmail") or ""),
            }
            normalized_fields = _stringify_fields(fields)
            results.append({
                "id": person_id,
                "jobId": job_id,
                "fields": normalized_fields,
            })
    finally:
        job_session.close()
    return results


def ch_get_person(person_id: str) -> Optional[Dict]:
    person_id = (person_id or "").strip()
    if not person_id:
        return None
    session = _new_session()
    try:
        url = f"{CH_API}/v2/org/{CH_ORG_ID}/person/{person_id}"
        payload = _get_json(session, url, {"include": "contacts,contact,fields"})
        entity = _extract_entity(payload)
        if not entity:
            return None
        fields = entity.get("fields")
        if isinstance(fields, dict):
            entity["fields"] = _stringify_fields(fields)
        return entity
    except HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            return None
        raise
    finally:
        session.close()

def ch_person_primary_email(person: Dict) -> str:
    if not isinstance(person, dict):
        return ""
    fields = person.get("fields") if isinstance(person.get("fields"), dict) else {}
    candidate_keys = [
        "contact workemail",
        "work email",
        "email",
        "contact email",
        "primary email",
        "person contact workemail",
        "contact personalemail",
        "personal email",
        "person contact personalemail",
    ]
    for key in candidate_keys:
        value = (fields.get(key) or "") if fields else ""
        if isinstance(value, str) and value.strip():
            return value.strip()

    flat_keys = [
        "contact.workEmail",
        "contact.email",
        "contact.personalEmail",
        "workEmail",
        "email",
    ]
    for key in flat_keys:
        value = (person.get(key) or "")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _normalize_timeoff_entry(
    entry: Dict,
    *,
    start_date: Optional[dt.date] = None,
    end_date: Optional[dt.date] = None,
) -> Optional[Dict]:
    fields_raw = dict(entry.get("fields") or {})
    start_raw = fields_raw.get("start date") or entry.get("startDate") or entry.get("start")
    end_raw = fields_raw.get("end date") or entry.get("endDate") or entry.get("end")
    if start_raw:
        fields_raw["start date"] = _norm_date_str(start_raw)
        fields_raw["startdate"] = _norm_date_str(start_raw)
    if end_raw:
        fields_raw["end date"] = _norm_date_str(end_raw)

    if "reason" not in fields_raw and entry.get("reason"):
        fields_raw["reason"] = entry.get("reason")
    if "type" not in fields_raw and entry.get("type"):
        fields_raw["type"] = entry.get("type")

    person_info = entry.get("person") or {}
    if isinstance(person_info, dict):
        person_fields = person_info.get("fields") or {}
        contact = person_info.get("contact") or {}
        work_email = (
            (person_fields.get("contact workemail") if isinstance(person_fields, dict) else None)
            or contact.get("workEmail")
            or contact.get("email")
        )
        personal_email = (
            (person_fields.get("contact personalemail") if isinstance(person_fields, dict) else None)
            or contact.get("personalEmail")
        )
        modern_email = _person_email(person_info)
        if modern_email and modern_email != work_email:
            work_email = work_email or modern_email
        if work_email:
            fields_raw.setdefault("person contact workemail", work_email)
            fields_raw.setdefault("contact workemail", work_email)
        if personal_email:
            fields_raw.setdefault("person contact personalemail", personal_email)
        elif modern_email:
            fields_raw.setdefault("person contact personalemail", modern_email)

    normalized_fields = _stringify_fields(fields_raw)
    entry_copy = dict(entry)
    entry_copy["fields"] = normalized_fields
    entry_copy["id"] = entry.get("id") or normalized_fields.get("id")

    start_dt = _parse_iso_date(normalized_fields.get("start date"))
    if start_dt is None:
        return None
    if start_date and start_dt < start_date:
        return None
    if end_date and start_dt > end_date:
        return None

    return entry_copy


def ch_fetch_timeoff_basic(start: str, end: str) -> List[Dict]:
    """GET /v1/org/{orgId}/timeoff con paginación, sin include."""
    url = f"{os.environ['CH_API']}/v1/org/{os.environ['CH_ORG_ID']}/timeoff"
    params: Dict[str, object] = {"limit": 200}
    if start:
        params["startDate[gte]"] = start
    if end:
        params["startDate[lte]"] = end
    return ch_get_paginated(url, params)


def _person_email(person: Dict) -> Optional[str]:
    """
    Prioriza WORK_EMAIL luego HOME_EMAIL en person.contacts,
    y si no existe, hace fallback a legacy person.contact.{workemail,personalemail}.
    """
    # Nuevo: contacts (API moderna)
    for typ in ("WORK_EMAIL", "HOME_EMAIL"):
        for contact in person.get("contacts", []) or []:
            if contact.get("type") == typ and contact.get("value"):
                return contact["value"]

    # Legacy: contact.*
    legacy_contact = person.get("contact") or {}
    for key in ("workemail", "personalemail"):
        value = legacy_contact.get(key)
        if value:
            return value

    return None


def ch_fetch_people_by_ids(ids: List[str]) -> Dict[str, Dict]:
    """Devuelve map personId -> {email, name, title} haciendo batch de 100 ids."""
    if not ids:
        return {}
    all_people: List[Dict] = []
    for i in range(0, len(ids), 100):
        batch = [str(pid) for pid in ids[i : i + 100] if pid]
        if not batch:
            continue
        chunk = ",".join(batch)
        url = f"{os.environ['CH_API']}/v1/org/{os.environ['CH_ORG_ID']}/person"
        params = {"ids": chunk, "include": "contact,contacts"}
        all_people += ch_get_paginated(url, params)
    pmap: Dict[str, Dict] = {}
    for person in all_people:
        email = _person_email(person)
        if email:
            pmap[person.get("id")] = {
                "email": email,
                "name": person.get("name"),
                "title": person.get("title"),
            }
    return pmap


def ch_fetch_timeoff(
    start: Optional[dt.date | dt.datetime], end: Optional[dt.date | dt.datetime]
) -> List[Dict]:
    start_date = _normalize_date_arg(start)
    end_date = _normalize_date_arg(end)
    url = f"{CH_API}/v1/org/{CH_ORG_ID}/timeoff"
    limit = CH_PEOPLE_PAGE_SIZE or 200
    base_params = {
        "limit": str(limit),
        "include": "person",
    }
    if start_date:
        base_params["startDate[gte]"] = start_date.isoformat()
    if end_date:
        base_params["startDate[lte]"] = end_date.isoformat()

    events: List[Dict] = []
    offset: Optional[str] = None
    session = _new_session()
    try:
        while True:
            params = dict(base_params)
            if offset:
                params["offset"] = offset
            payload = _get_json(session, url, params)
            data = payload.get("data") or []
            if isinstance(data, dict):
                data = [data]
            if not data:
                break

            for entry in data:
                normalized = _normalize_timeoff_entry(
                    entry,
                    start_date=start_date,
                    end_date=end_date,
                )
                if normalized:
                    events.append(normalized)

            next_token = payload.get("next")
            if not next_token:
                break
            offset = str(next_token)
        return events
    finally:
        session.close()


def ch_fetch_timeoff_enriched(start: str, end: str) -> List[Dict]:
    """Trae timeoff y enriquece cada item con personEmail/personName/personTitle."""
    items = ch_fetch_timeoff_basic(start, end)
    ids = sorted({it.get("personId") for it in items if it.get("personId")})
    pmap = ch_fetch_people_by_ids(ids)
    start_dt = _parse_iso_date(start)
    end_dt = _parse_iso_date(end)
    enriched: List[Dict] = []
    for raw in items:
        entry = dict(raw)
        person = pmap.get(entry.get("personId"))
        if person:
            entry["personEmail"] = person["email"]
            entry["personName"] = person.get("name")
            entry["personTitle"] = person.get("title")
        normalized = _normalize_timeoff_entry(entry, start_date=start_dt, end_date=end_dt)
        if normalized:
            if person:
                normalized.setdefault("personEmail", person["email"])
                if person.get("name"):
                    normalized.setdefault("personName", person["name"])
                if person.get("title"):
                    normalized.setdefault("personTitle", person["title"])
            enriched.append(normalized)
    return enriched


def ch_get_timeoff(timeoff_id: str) -> Optional[Dict]:
    timeoff_id = (timeoff_id or "").strip()
    if not timeoff_id:
        return None
    session = _new_session()
    try:
        url = f"{CH_API}/v1/org/{CH_ORG_ID}/timeoff/{timeoff_id}"
        payload = _get_json(session, url, params={"include": "person"})
        entry = payload.get("data") or payload
        if not isinstance(entry, dict):
            return None
        return _normalize_timeoff_entry(entry)
    finally:
        session.close()
