from __future__ import annotations

from typing import Dict

from app.clients.charthop import ch_import_people_csv, generate_unique_work_email
from app.clients.runn import runn_upsert_person
from app.clients.teamtailor import tt_get_offer_start_date_for_application
from app.utils.config import RUNN_CREATE_ON_HIRE


def _resolve_candidate(included: list, type_name: str) -> Dict:
    return next((item for item in included if item.get("type") == type_name), {}) or {}


def _extract_name(attributes: Dict, field: str) -> str:
    return attributes.get(field) or attributes.get(field.replace("-", "_")) or ""


def process_hired_application(app_id: str, payload: Dict) -> Dict:
    data = payload.get("data") or {}
    attributes = data.get("attributes") or {}
    status = (attributes.get("status") or attributes.get("state") or "").lower()
    hired_at = attributes.get("hired-at") or attributes.get("hired_at") or ""
    if status != "hired" and not hired_at:
        return {"processed": False, "reason": "application not hired"}

    included = payload.get("included") or []
    candidate = _resolve_candidate(included, "candidates")
    job = _resolve_candidate(included, "jobs")

    cand_attr = candidate.get("attributes") or {}
    job_attr = job.get("attributes") or {}

    first = _extract_name(cand_attr, "first-name")
    last = _extract_name(cand_attr, "last-name")
    personal_email = cand_attr.get("email") or ""
    title = job_attr.get("title") or ""

    start_date = tt_get_offer_start_date_for_application(app_id, payload) or (
        attributes.get("start-date")
        or attributes.get("start_date")
        or (hired_at or "")[:10]
    )

    work_email = generate_unique_work_email(first, last)

    row = {
        "first name": first,
        "last name": last,
        "contact personalemail": personal_email,
        "title": title,
        "start date": start_date or "",
    }
    if work_email:
        row["contact workemail"] = work_email

    ch_result = ch_import_people_csv([row])

    runn_result = None
    email_for_runn = work_email or personal_email or None
    if RUNN_CREATE_ON_HIRE and email_for_runn:
        runn_result = runn_upsert_person(
            name=f"{first} {last}".strip(),
            email=email_for_runn,
            employment_type="employee",
            starts_at=start_date,
        )

    return {
        "processed": True,
        "chartHopImport": ch_result,
        "generatedWorkEmail": work_email,
        "runnResult": runn_result,
    }
