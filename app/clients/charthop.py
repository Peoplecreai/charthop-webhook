import csv
import io
import time
from datetime import date, datetime
from typing import Dict, Iterable, Iterator, List, Optional
from urllib.parse import parse_qs, urlparse

import requests
from requests import Session
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import HTTPError, Timeout, SSLError

from app.utils.config import (
    CH_API,
    CH_CF_JOB_TT_ID_LABEL,
    CH_ORG_ID,
    CH_PEOPLE_PAGE_SIZE,
    CORP_EMAIL_DOMAIN,
    AUTO_ASSIGN_WORK_EMAIL,
    HTTP_TIMEOUT,
    ch_headers,
    compose_location,
    derive_locale_timezone,
    strip_accents_and_non_alnum,
)


def ch_find_job(job_id: str, fields: Optional[str] = None) -> Optional[Dict]:
    """Busca un Job por jobid usando el filtro q=jobid\\{id}."""
    params: Dict[str, str] = {"q": f"jobid\\{job_id}"}
    default_fields = ["title", "department name", "location name", "open"]
    if CH_CF_JOB_TT_ID_LABEL:
        default_fields.append(CH_CF_JOB_TT_ID_LABEL)
    params["fields"] = fields or ",".join(default_fields)
    try:
        r = requests.get(
            f"{CH_API}/v2/org/{CH_ORG_ID}/job",
            headers=ch_headers(),
            params=params,
            timeout=HTTP_TIMEOUT,
        )
    except Exception as exc:  # pragma: no cover - logging
        print("ch_find_job error:", repr(exc))
        return None
    if not r.ok:
        print("ch_find_job status:", r.status_code, (r.text or "")[:200])
        return None
    items = (r.json() or {}).get("data") or []
    return items[0] if items else None


def ch_upsert_job_field(job_id: str, field_label: str, value: str):
    sio = io.StringIO()
    writer = csv.DictWriter(sio, fieldnames=["job id", field_label])
    writer.writeheader()
    writer.writerow({"job id": job_id, field_label: value})
    sio.seek(0)
    files = {"file": ("jobs.csv", sio.read())}
    url = f"{CH_API}/v1/org/{CH_ORG_ID}/import/csv/data"
    params = {"upsert": "true"}
    r = requests.post(
        url,
        headers=ch_headers(),
        params=params,
        files=files,
        timeout=HTTP_TIMEOUT,
    )
    r.raise_for_status()
    return r.json()


def ch_import_people_csv(rows: List[Dict]):
    if not rows:
        return {"status": "empty"}
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    output.seek(0)
    files = {"file": ("people.csv", output.read())}
    url = f"{CH_API}/v1/org/{CH_ORG_ID}/import/csv/data"
    params = {"upsert": "true", "creategroups": "true"}
    r = requests.post(
        url,
        headers=ch_headers(),
        params=params,
        files=files,
        timeout=HTTP_TIMEOUT,
    )
    r.raise_for_status()
    return r.json()


def ch_email_exists(email: str) -> bool:
    if not email:
        return False
    try:
        url = f"{CH_API}/v2/org/{CH_ORG_ID}/person"
        params = {"q": f"contact workemail\\{email}", "fields": "contact workemail"}
        r = requests.get(url, headers=ch_headers(), params=params, timeout=HTTP_TIMEOUT)
        if not r.ok:
            return False
        for item in (r.json() or {}).get("data", []):
            fields = item.get("fields") or {}
            work = (fields.get("contact workemail") or "").strip().lower()
            if work == email.strip().lower():
                return True
    except Exception as exc:  # pragma: no cover - logging
        print("ch_email_exists error:", repr(exc))
    return False


def generate_unique_work_email(first: str, last: str) -> Optional[str]:
    if not AUTO_ASSIGN_WORK_EMAIL or not CORP_EMAIL_DOMAIN:
        return None
    base_local = f"{strip_accents_and_non_alnum(first)}{strip_accents_and_non_alnum(last)}"
    if not base_local:
        return None
    candidate = f"{base_local}@{CORP_EMAIL_DOMAIN}"
    if not ch_email_exists(candidate):
        return candidate
    for i in range(2, 100):
        candidate = f"{base_local}{i}@{CORP_EMAIL_DOMAIN}"
        if not ch_email_exists(candidate):
            return candidate
    return None


def _coerce_int(value) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_offset_from_link(link: Optional[str]) -> Optional[int]:
    if not link:
        return None
    try:
        parsed = urlparse(link)
    except Exception:  # pragma: no cover - defensive
        return None
    query_params = parse_qs(parsed.query)
    for key in ("offset", "page[offset]", "page%5Boffset%5D"):
        values = query_params.get(key)
        if not values:
            continue
        for raw in reversed(values):
            candidate = _coerce_int(raw)
            if candidate is not None:
                return candidate
    return None


def _resolve_next_offset(payload: Dict, current_offset: int, page_size: int, data_len: int) -> Optional[int]:
    candidate_sources = [
        payload.get("nextOffset"),
        payload.get("next_offset"),
    ]
    meta = payload.get("meta") if isinstance(payload, dict) else None
    pagination = None
    if isinstance(meta, dict):
        candidate_sources.extend([meta.get("nextOffset"), meta.get("next_offset")])
        pagination = meta.get("pagination")
        if isinstance(pagination, dict):
            candidate_sources.extend(
                [
                    pagination.get("nextOffset"),
                    pagination.get("next_offset"),
                ]
            )

    for source in candidate_sources:
        parsed = _coerce_int(source)
        if parsed is not None:
            return parsed

    links = payload.get("links") if isinstance(payload, dict) else None
    if isinstance(links, dict):
        linked = _extract_offset_from_link(links.get("next"))
        if linked is not None:
            return linked

    linked = _extract_offset_from_link(payload.get("next")) if isinstance(payload, dict) else None
    if linked is not None:
        return linked

    def _boolish(value) -> Optional[bool]:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"true", "1", "yes", "on"}:
                return True
            if lowered in {"false", "0", "no", "off"}:
                return False
        if isinstance(value, (int, float)):
            return bool(value)
        return None

    bool_sources = []
    if isinstance(pagination, dict):
        bool_sources.extend(
            [
                pagination.get("hasMore"),
                pagination.get("has_more"),
            ]
        )
    if isinstance(payload, dict):
        bool_sources.extend(
            [
                payload.get("hasMore"),
                payload.get("has_more"),
                payload.get("more"),
            ]
        )
    for source in bool_sources:
        flag = _boolish(source)
        if flag is False:
            return None

    total_sources = []
    if isinstance(pagination, dict):
        total_sources.extend(
            [
                pagination.get("total"),
                pagination.get("count"),
                pagination.get("records"),
            ]
        )
    if isinstance(payload, dict):
        total_sources.append(payload.get("total"))
    for source in total_sources:
        total = _coerce_int(source)
        if total is not None:
            if current_offset + data_len >= total:
                return None
            break

    if data_len < page_size:
        return None

    return current_offset + data_len


def ch_iter_people(fields: str, limit: Optional[int] = None, max_retries: int = 5) -> Iterator[Dict]:
    page_size = limit or CH_PEOPLE_PAGE_SIZE
    if page_size <= 0:
        page_size = 200
    offset = 0
    seen_offsets = set()
    headers = ch_headers()
    with Session() as session:
        session.headers.update(headers)
        adapter = HTTPAdapter(pool_connections=4, pool_maxsize=4, max_retries=0)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        while True:
            if offset in seen_offsets:
                print("ch_iter_people detected repeated offset:", offset)
                return
            params = {"fields": fields, "limit": page_size, "offset": offset}
            attempt = 0
            last_exc: Optional[Exception] = None
            while True:
                try:
                    response = session.get(
                        f"{CH_API}/v2/org/{CH_ORG_ID}/person",
                        params=params,
                        timeout=HTTP_TIMEOUT,
                    )
                except (RequestsConnectionError, Timeout, SSLError) as exc:  # pragma: no cover - network noise
                    attempt += 1
                    last_exc = exc
                else:
                    if response.status_code == 429:
                        attempt += 1
                        last_exc = HTTPError("429 Too Many Requests", response=response)
                    else:
                        try:
                            response.raise_for_status()
                        except HTTPError as exc:
                            status = exc.response.status_code if exc.response is not None else None
                            if status is not None and 400 <= status < 500 and status not in (408, 429):
                                body = ""
                                if exc.response is not None:
                                    body = (exc.response.text or "")[:200]
                                lowered = body.lower()
                                if (
                                    page_size > 200
                                    and lowered
                                    and any(token in lowered for token in ("limit", "page size", "page_size"))
                                ):
                                    page_size = 200
                                    params["limit"] = page_size
                                    attempt = 0
                                    last_exc = None
                                    continue
                                print("ch_iter_people status:", status, body)
                                return
                            attempt += 1
                            last_exc = exc
                        else:
                            try:
                                payload = response.json() or {}
                            except ValueError as exc:  # pragma: no cover - invalid JSON edge
                                attempt += 1
                                last_exc = exc
                                continue
                            break
                if attempt > max_retries:
                    print("ch_iter_people error:", repr(last_exc))
                    return
                sleep_for = min(2 ** (attempt - 1), 30)
                time.sleep(sleep_for)

            data = payload.get("data") if isinstance(payload, dict) else None
            if not data:
                return
            if isinstance(data, dict):
                data = [data]
            elif not isinstance(data, list):
                data = list(data)  # ensure len() works even if ChartHop returns generator-like
            if not data:
                return
            seen_offsets.add(offset)

            for item in data:
                yield item

            data_len = len(data)
            next_offset = _resolve_next_offset(payload, offset, page_size, data_len)
            if next_offset is None:
                return
            if next_offset == offset:
                print("ch_iter_people stalled offset:", offset)
                return
            offset = next_offset


def ch_active_people(fields: str, page_size: Optional[int] = None) -> Iterator[Dict]:
    for item in ch_iter_people(fields, limit=page_size):
        fields_map = item.get("fields") or {}
        status = (fields_map.get("status") or "").strip().lower()
        if status and status not in {"active", "current", "enabled"}:
            continue
        yield item


def ch_people_starting_between(start: date, end: date, fields: Optional[str] = None) -> List[Dict]:
    fields = fields or "person id,name first,name last,contact workemail,contact personalemail,start date,title"
    people = []
    for item in ch_active_people(fields + ",status"):
        flds = item.get("fields") or {}
        start_raw = (flds.get("start date") or flds.get("startdate") or "").strip()
        if not start_raw:
            continue
        try:
            start_dt = datetime.strptime(start_raw[:10], "%Y-%m-%d").date()
        except ValueError:
            continue
        if start <= start_dt <= end:
            people.append(item)
    return people


def ch_fetch_timeoff(start: date, end: date) -> List[Dict]:
    url = f"{CH_API}/v2/org/{CH_ORG_ID}/timeoff"
    params = {
        "startDate": start.isoformat(),
        "endDate": end.isoformat(),
        "fields": "person id,person name,person contact workemail,start date,end date,type,reason,status",
    }
    try:
        r = requests.get(url, headers=ch_headers(), params=params, timeout=HTTP_TIMEOUT)
        if not r.ok:
            print("ch_fetch_timeoff status:", r.status_code, (r.text or "")[:200])
            return []
        payload = r.json() or {}
        data = payload.get("data")
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return [data]
    except Exception as exc:  # pragma: no cover - logging
        print("ch_fetch_timeoff error:", repr(exc))
    return []


def _norm_date(s: str) -> str:
    """Normaliza a YYYY-MM-DD si viene en formato ISO; en otros casos la deja tal cual."""
    s = (s or "").strip()
    if not s:
        return ""
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        return s


def build_culture_amp_rows() -> List[Dict[str, str]]:
    return list(iter_culture_amp_rows())


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


def iter_culture_amp_rows() -> Iterator[Dict[str, str]]:
    fields = ",".join(
        [
            "employee id",                # preferido para Employee Id
            "person id",                  # fallback
            "name first",
            "name last",
            "preferred name first",
            "preferred name last",        # NUEVO: para armar Name con preferred
            "contact workemail",
            "contact personalemail",
            "manager contact workemail",
            "title",
            "seniority",
            "homeaddress country",
            "homeaddress region",
            "homeaddress city",           # NUEVO: Location = ciudad
            "status",
            "start date",                 # Start Date
            "end date",                   # End Date
            "department",                 # Department
            "department name",            # fallback
            "employment",                 # Employment Type
        ]
    )
    for person in ch_active_people(fields):
        flds = person.get("fields") or {}

        work = (flds.get("contact workemail") or "").strip()
        if not work:
            continue

        # Name: preferidos primero (first + last), luego first/last normales
        pref_first = (flds.get("preferred name first") or "").strip()
        pref_last = (flds.get("preferred name last") or "").strip()
        first = (flds.get("name first") or "").strip()
        last = (flds.get("name last") or "").strip()

        if pref_first or pref_last:
            name = " ".join(p for p in [pref_first, pref_last] if p).strip()
        else:
            name = " ".join(p for p in [first, last] if p).strip()

        # Preferred Name (sigue siendo preferred first)
        preferred_display = pref_first

        # Location: solo la ciudad del home address
        city = (flds.get("homeaddress city") or "").strip()

        # Employee Id preferido, luego person id, luego email
        employee_id = (
            (flds.get("employee id") or "").strip()
            or (flds.get("person id") or "").strip()
            or work
        )

        start_raw = (flds.get("start date") or flds.get("startdate") or "").strip()
        end_raw = (flds.get("end date") or flds.get("enddate") or "").strip()
        department = (flds.get("department") or flds.get("department name") or "").strip()
        country = (flds.get("homeaddress country") or "").strip()
        employment = (flds.get("employment") or "").strip()
        region = (flds.get("homeaddress region") or "").strip()  # se mantiene por si lo usas en otra parte

        yield {
            "Employee Id": employee_id,
            "Email": work,
            "Name": name,
            "Preferred Name": preferred_display,
            "Manager Email": flds.get("manager contact workemail") or "",
            "Location": city,
            "Job Title": flds.get("title") or "",
            "Seniority": flds.get("seniority") or "",
            "Start Date": _norm_date(start_raw),
            "End Date": _norm_date(end_raw),
            "Department": department,
            "Country": country,
            "Employment Type": employment,
        }


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


def ch_person_primary_email(person: Dict) -> Optional[str]:
    flds = (person or {}).get("fields") or {}
    work = (flds.get("contact workemail") or "").strip()
    if work:
        return work
    personal = (flds.get("contact personalemail") or "").strip()
    return personal or None
