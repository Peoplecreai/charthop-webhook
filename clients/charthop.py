import io, csv, requests
from . import __init__  # noqa
from app.utils.config import CH_API, CH_ORG_ID, ch_headers, HTTP_TIMEOUT

def ch_find_job(job_id: str):
    url = f"{CH_API}/v2/org/{CH_ORG_ID}/job"
    params = {"q": f"jobid\\{job_id}", "fields": "title,department name,location name,open"}
    r = requests.get(url, headers=ch_headers(), params=params, timeout=HTTP_TIMEOUT)
    if not r.ok:
        print("ch_find_job status:", r.status_code, (r.text or "")[:200])
        return None
    items = (r.json() or {}).get("data") or []
    return items[0] if items else None

def ch_upsert_job_field(job_id: str, field_label: str, value: str):
    sio = io.StringIO()
    w = csv.DictWriter(sio, fieldnames=["job id", field_label])
    w.writeheader()
    w.writerow({"job id": job_id, field_label: value})
    sio.seek(0)
    files = {"file": ("jobs.csv", sio.read())}
    url = f"{CH_API}/v1/org/{CH_ORG_ID}/import/csv/data"
    params = {"upsert": "true"}
    r = requests.post(url, headers=ch_headers(), params=params, files=files, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()

def ch_import_people_csv(rows):
    if not rows:
        return {"status": "empty"}
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=rows[0].keys())
    writer.writeheader(); writer.writerows(rows); output.seek(0)
    files = {"file": ("people.csv", output.read())}
    url = f"{CH_API}/v1/org/{CH_ORG_ID}/import/csv/data"
    params = {"upsert": "true", "creategroups": "true"}
    r = requests.post(url, headers=ch_headers(), params=params, files=files, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()

