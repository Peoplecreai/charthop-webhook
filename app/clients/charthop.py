import csv
import io
from datetime import date, datetime
from typing import Dict, Iterable, Iterator, List, Optional

import requests

from app.utils.config import (
    CH_API,
    CH_CF_JOB_TT_ID_LABEL,
    CH_ORG_ID,
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


def ch_iter_people(fields: str, limit: int = 200) -> Iterator[Dict]:
    offset = 0
    while True:
        params = {"fields": fields, "limit": limit, "offset": offset}
        try:
            r = requests.get(
                f"{CH_API}/v2/org/{CH_ORG_ID}/person",
                headers=ch_headers(),
                params=params,
                timeout=HTTP_TIMEOUT,
            )
            r.raise_for_status()
        except Exception as exc:  # pragma: no cover - logging
            print("ch_iter_people error:", repr(exc))
            return
        payload = r.json() or {}
        data = payload.get("data") or []
        if not data:
            return
        for item in data:
            yield item
        offset += len(data)


def ch_active_people(fields: str) -> List[Dict]:
    rows: List[Dict] = []
    for item in ch_iter_people(fields):
        fields_map = item.get("fields") or {}
        status = (fields_map.get("status") or "").strip().lower()
        if status and status not in {"active", "current", "enabled"}:
            continue
        rows.append(item)
    return rows


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


def build_culture_amp_rows() -> List[Dict[str, str]]:
    fields = ",".join(
        [
            "person id",
            "name first",
            "name last",
            "preferred name first",
            "contact workemail",
            "contact personalemail",
            "manager contact workemail",
            "title",
            "seniority",
            "homeaddress country",
            "homeaddress region",
            "status",
        ]
    )
    rows: List[Dict[str, str]] = []
    for person in ch_active_people(fields):
        flds = person.get("fields") or {}
        work = (flds.get("contact workemail") or "").strip()
        if not work:
            continue
        preferred = flds.get("preferred name first") or ""
        first = preferred or (flds.get("name first") or "")
        last = flds.get("name last") or ""
        name = " ".join(part for part in [first, last] if part).strip()
        country = flds.get("homeaddress country") or ""
        region = flds.get("homeaddress region") or ""
        locale, timezone = derive_locale_timezone(country)
        rows.append(
            {
                "Employee Id": flds.get("person id") or work,
                "Email": work,
                "Name": name,
                "Preferred Name": preferred,
                "Manager Email": flds.get("manager contact workemail") or "",
                "Location": compose_location(region, country),
                "Job Title": flds.get("title") or "",
                "Seniority": flds.get("seniority") or "",
                "Locale": locale,
                "Timezone": timezone,
            }
        )
    return rows


def culture_amp_csv_from_rows(rows: Iterable[Dict[str, str]]) -> str:
    columns = [
        "Employee Id",
        "Email",
        "Name",
        "Preferred Name",
        "Manager Email",
        "Location",
        "Job Title",
        "Seniority",
        "Locale",
        "Timezone",
    ]
    sio = io.StringIO()
    writer = csv.DictWriter(sio, fieldnames=columns, extrasaction="ignore")
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
