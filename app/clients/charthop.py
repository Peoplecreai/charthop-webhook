from __future__ import annotations

import csv
import io
import time
from typing import Dict, Iterable, Iterator, List, Optional

from requests import Session
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import HTTPError, Timeout, SSLError

from app.utils.config import (
    CH_API,
    CH_ORG_ID,
    CH_PEOPLE_PAGE_SIZE,
    HTTP_TIMEOUT,
    ch_headers,
)

# ---------- Utilidades internas ----------

def _new_session() -> Session:
    s = Session()
    s.headers.update(ch_headers())
    # pool chico pero estable; reintentos manejados a mano
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


# ---------- Iteradores v2 (person) con cursor ----------

# Campos que necesitamos para Culture Amp, usando rutas con punto (v2)
PEOPLE_FIELDS = ",".join(
    [
        "id",                          # backup Employee Id
        "contact.employee",            # preferred Employee Id (si lo tienen)
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
    ]
)

def ch_iter_people_v2(fields: str = PEOPLE_FIELDS, page_size: Optional[int] = None) -> Iterator[Dict]:
    url = f"{CH_API}/v2/org/{CH_ORG_ID}/person"
    session = _new_session()
    limit = page_size or CH_PEOPLE_PAGE_SIZE or 200
    if limit <= 0:
        limit = 200

    offset: Optional[str] = None
    seen_offsets = set()

    while True:
        params = {
            "fields": fields,
            "limit": str(limit),
            # includeAll=false => solo plantilla vigente (evita históricos/terminados)
            "includeAll": "false",
        }
        if offset:
            if offset in seen_offsets:
                # Evita loops si el cursor se repite
                break
            params["offset"] = offset

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
        seen_offsets.add(offset or "")
        offset = str(next_token)


# ---------- Lookup de Employment Type desde Job ----------

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


# ---------- Construcción de rows para Culture Amp ----------

CULTURE_AMP_COLUMNS = [
    "Employee Id",
    "Email",
    "Name",
    "Preferred Name",
    "Manager Email",
    "Location",
    "Job Title",
    "Seniority",
    "Start Date",
    "End Date",
    "Department",
    "Country",
    "Employment Type",
]

def _norm_date_str(s: Optional[str]) -> str:
    s = (s or "").strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    return s

def iter_culture_amp_rows() -> Iterator[Dict[str, str]]:
    """
    Devuelve rows ya mapeadas con los headers de Culture Amp.
    Employment Type se resuelve por jobId con cache simple por proceso.
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
            pref_first = (p.get("name.pref") or "").strip()
            pref_last = (p.get("name.preflast") or "").strip()
            first = (p.get("name.first") or "").strip()
            last = (p.get("name.last") or "").strip()

            if pref_first or pref_last:
                name = f"{pref_first} {pref_last}".strip()
            else:
                name = f"{first} {last}".strip()

            manager_email = (p.get("manager.contact.workEmail") or "").strip()
            city = (p.get("address.city") or "").strip()
            country = (p.get("address.country") or "").strip()
            title = (p.get("title") or "").strip()
            seniority = (p.get("seniority") or "").strip()
            start_date = _norm_date_str(p.get("startDateOrg"))
            end_date = _norm_date_str(p.get("endDateOrg"))
            department = (p.get("department.name") or "").strip()

            job_id = (p.get("jobId") or "").strip()
            if job_id:
                if job_id in job_cache:
                    employment = job_cache[job_id] or ""
                else:
                    employment = ch_get_job_employment(job_id, session=session) or ""
                    job_cache[job_id] = employment
            else:
                employment = ""

            yield {
                "Employee Id": emp_id,
                "Email": email,
                "Name": name,
                "Preferred Name": pref_first,
                "Manager Email": manager_email,
                "Location": city,
                "Job Title": title,
                "Seniority": seniority,
                "Start Date": start_date,
                "End Date": end_date,
                "Department": department,
                "Country": country,
                "Employment Type": employment,
            }
    finally:
        session.close()
