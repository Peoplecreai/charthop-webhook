import os, io, csv, hmac, hashlib, base64, datetime as dt, unicodedata
from flask import Flask, request, abort
import requests
import paramiko

app = Flask(__name__)

# =========================
# Config / Entorno
# =========================
CH_API = os.getenv("CH_API", "https://api.charthop.com")
CH_ORG_ID = os.getenv("CH_ORG_ID")
CH_API_TOKEN = os.getenv("CH_API_TOKEN")

TT_API = os.getenv("TT_API", "https://api.teamtailor.com/v1")
TT_API_KEY = os.getenv("TT_API_KEY") or os.getenv("TEAMTAILOR_API_KEY")
TT_API_VERSION = os.getenv("TT_API_VERSION", "20240404")  # usa una versión estable
TT_SIGNATURE_KEY = os.getenv("TT_SIGNATURE_KEY")

RUNN_API = os.getenv("RUNN_API", "https://api.runn.io")
RUNN_API_TOKEN = os.getenv("RUNN_API_TOKEN")
RUNN_API_VERSION = os.getenv("RUNN_API_VERSION", "1.0.0")

CA_SFTP_HOST = os.getenv("CA_SFTP_HOST")
CA_SFTP_USER = os.getenv("CA_SFTP_USER")
CA_SFTP_PASS = os.getenv("CA_SFTP_PASS")                   # si usas password
CA_SFTP_KEY = os.getenv("CA_SFTP_KEY")                     # privada PEM (Secret)
CA_SFTP_PASSPHRASE = os.getenv("CA_SFTP_PASSPHRASE")       # opcional
CA_SFTP_PATH = os.getenv("CA_SFTP_PATH", "/upload")

DEFAULT_LOCALE = os.getenv("DEFAULT_LOCALE", "es-LA")
DEFAULT_TIMEZONE = os.getenv("DEFAULT_TIMEZONE", "UTC")
CORP_EMAIL_DOMAIN = os.getenv("CORP_EMAIL_DOMAIN", "creai.mx")

HTTP_TIMEOUT = 30

# =========================
# Headers
# =========================
def ch_headers():
    return {"Authorization": f"Bearer {CH_API_TOKEN}"}

def tt_headers():
    return {
        "Authorization": f"Token token={TT_API_KEY}",
        "X-Api-Version": TT_API_VERSION,
        "Content-Type": "application/vnd.api+json"
    }

def runn_headers():
    return {
        "Authorization": f"Bearer {RUNN_API_TOKEN}",
        "Accept-Version": RUNN_API_VERSION,
        "Content-Type": "application/json"
    }

# =========================
# Helpers generales
# =========================
def tt_verify_signature(payload: dict):
    # Teamtailor: HMAC-SHA256(resource_id) -> hex -> base64
    if not TT_SIGNATURE_KEY:
        return True
    provided = request.headers.get("Teamtailor-Signature", "")
    resource_id = str(payload.get("resource_id") or payload.get("id") or "")
    mac_hex = hmac.new(TT_SIGNATURE_KEY.encode(), resource_id.encode(), hashlib.sha256).hexdigest()
    expected = base64.b64encode(mac_hex.encode()).decode()
    if not hmac.compare_digest(provided, expected):
        abort(401, "Invalid Teamtailor signature")
    return True

def _strip_accents_and_non_alnum(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode("ascii")
    return "".join(ch for ch in s.lower() if ch.isalnum())

def derive_locale_timezone(country_code: str):
    if not country_code:
        return DEFAULT_LOCALE, DEFAULT_TIMEZONE
    cc = country_code.strip().upper()
    locale_map = {
        "MX": "es-MX","CR": "es-CR","CO": "es-CO","AR": "es-AR","CL": "es-CL",
        "US": "en-US","ES": "es-ES","BR": "pt-BR",
    }
    tz_map = {
        "MX": "America/Mexico_City","CR": "America/Costa_Rica","CO": "America/Bogota",
        "AR": "America/Argentina/Buenos_Aires","CL": "America/Santiago","US": "America/Los_Angeles",
        "ES": "Europe/Madrid","BR": "America/Sao_Paulo",
    }
    return locale_map.get(cc, DEFAULT_LOCALE), tz_map.get(cc, DEFAULT_TIMEZONE)

def compose_location(state_or_city: str, country_code: str):
    state = (state_or_city or "").strip()
    cc = (country_code or "").strip()
    if state and cc: return f"{state}, {cc}"
    if cc: return cc
    return ""

# =========================
# ChartHop helpers
# =========================
def ch_find_job(job_id: str):
    url = f"{CH_API}/v2/org/{CH_ORG_ID}/job"
    params = {"q": f"jobid\\{job_id}", "fields": "title,department name,location name,open"}
    r = requests.get(url, headers=ch_headers(), params=params, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    items = (r.json() or {}).get("data") or []
    return items[0] if items else None

def ch_email_exists(email: str) -> bool:
    if not email: return False
    try:
        url = f"{CH_API}/v2/org/{CH_ORG_ID}/job"
        params = {"q": f"contact workemail\\{email}", "fields": "contact workemail"}
        r = requests.get(url, headers=ch_headers(), params=params, timeout=HTTP_TIMEOUT)
        if not r.ok: return False
        for it in (r.json() or {}).get("data", []):
            wf = (it.get("fields") or {}).get("contact workemail") or ""
            if wf.strip().lower() == email.strip().lower():
                return True
        return False
    except Exception as e:
        print("ch_email_exists error:", e)
        return False

def generate_unique_work_email(first: str, last: str) -> str | None:
    base_local = f"{_strip_accents_and_non_alnum(first)}{_strip_accents_and_non_alnum(last)}"
    if not base_local: return None
    cand = f"{base_local}@{CORP_EMAIL_DOMAIN}"
    if not ch_email_exists(cand): return cand
    for i in range(2, 100):
        cand2 = f"{base_local}{i}@{CORP_EMAIL_DOMAIN}"
        if not ch_email_exists(cand2): return cand2
    return None

def ch_import_people_csv(rows):
    if not rows: return {"status": "empty"}
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=rows[0].keys())
    writer.writeheader(); writer.writerows(rows); output.seek(0)
    files = {"file": ("people.csv", output.read())}
    url = f"{CH_API}/v1/org/{CH_ORG_ID}/import/csv/data"
    params = {"upsert": "true", "creategroups": "true"}
    r = requests.post(url, headers=ch_headers(), params=params, files=files, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()

# =========================
# Runn helpers
# =========================
def runn_upsert_person(name, email, role_id=None, team_id=None, employment_type=None, starts_at=None):
    payload = {"name": name}
    if email: payload["email"] = email
    if role_id: payload["role_id"] = role_id
    if team_id: payload["team_id"] = team_id
    if employment_type: payload["employment_type"] = employment_type
    if starts_at: payload["starts_at"] = starts_at
    r = requests.post(f"{RUNN_API}/people", headers=runn_headers(), json=payload, timeout=HTTP_TIMEOUT)
    if r.status_code in (200, 201):
        return r.json()
    if r.status_code == 409 and email:
        q = requests.get(f"{RUNN_API}/people", headers=runn_headers(), params={"email": email}, timeout=HTTP_TIMEOUT)
        if q.ok and isinstance(q.json(), list) and q.json():
            pid = (q.json()[0] or {}).get("id")
            requests.patch(f"{RUNN_API}/people/{pid}", headers=runn_headers(), json=payload, timeout=HTTP_TIMEOUT)
            return {"id": pid}
    r.raise_for_status()

def runn_create_leave(person_id, starts_at, ends_at, reason="Vacation", external_ref=None):
    payload = {"personId": person_id, "startsAt": starts_at, "endsAt": ends_at, "reason": reason}
    if external_ref: payload["externalRef"] = external_ref
    r = requests.post(f"{RUNN_API}/time-offs/leave", headers=runn_headers(), json=payload, timeout=HTTP_TIMEOUT)
    r.raise_for_status(); return r.json()

# =========================
# Export nocturno a Culture Amp
# =========================
def build_ca_csv_from_charthop():
    """
    CSV para Culture Amp:
    Employee Id, Email, Name, Preferred Name, Manager Email, Location,
    Job Title, Seniority, Locale, Timezone
    Solo usa work email. Si no hay, omite la fila.
    """
    url = f"{CH_API}/v2/org/{CH_ORG_ID}/job"
    params = {
        "q": "open\\filled",
        "fields": ",".join([
            "jobId","person id","name first","name last","preferred name first",
            "contact workemail","contact personalemail",   # personal se ignora aquí
            "manager contact workemail","title","seniority",
            "homeaddress country","homeaddress region",
        ])
    }
    r = requests.get(url, headers=ch_headers(), params=params, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    rows = []
    for item in (r.json() or {}).get("data", []):
        f = item.get("fields") or {}
        first_pref = f.get("preferred name first") or ""
        first = first_pref or f.get("name first") or ""
        last = f.get("name last") or ""
        name = f"{first} {last}".strip() if first or last else ""
        work = f.get("contact workemail") or ""
        if not work:
            continue
        manager_email = f.get("manager contact workemail") or ""
        country = f.get("homeaddress country") or ""
        region = f.get("homeaddress region") or ""
        locale, tz = derive_locale_timezone(country)
        location = compose_location(region, country)
        employee_id = f.get("person id") or work
        rows.append({
            "Employee Id": employee_id,
            "Email": work,
            "Name": name,
            "Preferred Name": first_pref or "",
            "Manager Email": manager_email,
            "Location": location,
            "Job Title": f.get("title") or "",
            "Seniority": f.get("seniority") or "",
            "Locale": locale,
            "Timezone": tz,
        })
    cols = ["Employee Id","Email","Name","Preferred Name","Manager Email",
            "Location","Job Title","Seniority","Locale","Timezone"]
    sio = io.StringIO(); w = csv.DictWriter(sio, fieldnames=cols, extrasaction="ignore")
    w.writeheader(); [w.writerow(rw) for rw in rows]
    return sio.getvalue()

def sftp_upload(host, username, password=None, pkey_pem=None, remote_path="/upload/employee_import.csv", content=""):
    """
    Soporta Ed25519 y RSA. pkey_pem debe ser la privada PEM (desde Secret).
    """
    key = None
    if pkey_pem:
        buf = io.StringIO(pkey_pem)
        try:
            key = paramiko.Ed25519Key.from_private_key(buf, password=CA_SFTP_PASSPHRASE)
        except Exception:
            buf.seek(0)
            key = paramiko.RSAKey.from_private_key(buf, password=CA_SFTP_PASSPHRASE)
    transport = paramiko.Transport((host, 22))
    if key:
        transport.connect(username=username, pkey=key)
    else:
        transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    with sftp.file(remote_path, "w") as f:
        f.write(content)
    sftp.close(); transport.close()

# =========================
# Webhooks
# =========================
@app.route("/webhooks/teamtailor", methods=["POST"])
def tt_webhook():
    payload = request.get_json(force=True, silent=True) or {}
    tt_verify_signature(payload)

    resource_id = payload.get("resource_id") or payload.get("id")
    if not resource_id:
        return "", 200

    try:
        resp = requests.get(
            f"{TT_API}/job-applications/{resource_id}?include=candidate,job",
            headers=tt_headers(), timeout=HTTP_TIMEOUT
        )
    except Exception as e:
        print("TT fetch error:", e); return "", 200
    if not resp.ok: return "", 200

    body = resp.json() or {}
    data = body.get("data") or {}
    attributes = data.get("attributes") or {}
    status = (attributes.get("status") or attributes.get("state") or "").lower()
    hired_at = attributes.get("hired-at") or attributes.get("hired_at")
    if status != "hired" and not hired_at:
        return "", 200

    included = body.get("included") or []
    candidate = next((i for i in included if i.get("type") == "candidates"), {}) or {}
    job = next((i for i in included if i.get("type") == "jobs"), {}) or {}

    cand_attr = candidate.get("attributes") or {}
    job_attr = job.get("attributes") or {}

    first = cand_attr.get("first-name") or cand_attr.get("first_name") or ""
    last = cand_attr.get("last-name") or cand_attr.get("last_name") or ""
    personal_email = cand_attr.get("email") or ""  # solo se guarda en ChartHop
    title = job_attr.get("title") or ""
    start_date = attributes.get("start-date") or attributes.get("start_date") or (hired_at or "")[:10]

    work_email = generate_unique_work_email(first, last)

    # 1) ChartHop: personal email SOLO aquí + work email definitivo
    rows = [{
        "personal email": personal_email,
        **({"work email": work_email} if work_email else {}),
        "first name": first, "last name": last,
        "title": title, "start date": start_date
    }]
    try:
        ch_import_people_csv(rows)
    except Exception as e:
        print("ChartHop import error:", e)

    # 2) Runn: SOLO work email
    try:
        if work_email:
            runn_upsert_person(
                name=f"{first} {last}".strip(),
                email=work_email,
                employment_type="employee",
                starts_at=start_date
            )
