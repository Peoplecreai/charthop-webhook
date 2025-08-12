import os, io, csv, hmac, hashlib, base64, json, datetime as dt, unicodedata
from flask import Flask, request, abort
import requests
import paramiko

app = Flask(__name__)

# =========================
# Configuración por entorno
# =========================
CH_API = os.getenv("CH_API", "https://api.charthop.com")
CH_ORG_ID = os.getenv("CH_ORG_ID")
CH_API_TOKEN = os.getenv("CH_API_TOKEN")

TT_API = os.getenv("TT_API", "https://api.teamtailor.com/v1")
TT_API_KEY = os.getenv("TT_API_KEY")
TT_API_VERSION = os.getenv("TT_API_VERSION", "20240404")
TT_SIGNATURE_KEY = os.getenv("TT_SIGNATURE_KEY")

RUNN_API = os.getenv("RUNN_API", "https://api.runn.io")
RUNN_API_TOKEN = os.getenv("RUNN_API_TOKEN")
RUNN_API_VERSION = os.getenv("RUNN_API_VERSION", "1.0.0")

CA_SFTP_HOST = os.getenv("CA_SFTP_HOST")
CA_SFTP_USER = os.getenv("CA_SFTP_USER")
CA_SFTP_PASS = os.getenv("CA_SFTP_PASS")
CA_SFTP_KEY = os.getenv("CA_SFTP_KEY")
CA_SFTP_PATH = os.getenv("CA_SFTP_PATH", "/upload")

DEFAULT_LOCALE = os.getenv("DEFAULT_LOCALE", "es-LA")
DEFAULT_TIMEZONE = os.getenv("DEFAULT_TIMEZONE", "UTC")
CORP_EMAIL_DOMAIN = os.getenv("CORP_EMAIL_DOMAIN", "creai.mx")


# =========================
# Headers de cada servicio
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

def ch_email_exists(email: str) -> bool:
    """
    Verifica en ChartHop si ya existe un 'contact workemail' igual (case-insensitive).
    """
    if not email:
        return False
    try:
        url = f"{CH_API}/v2/org/{CH_ORG_ID}/job"
        params = {"q": f"contact workemail\\{email}", "fields": "contact workemail"}
        r = requests.get(url, headers=ch_headers(), params=params, timeout=15)
        if not r.ok:
            return False
        items = r.json().get("data") or []
        for it in items:
            f = it.get("fields", {}) or {}
            if (f.get("contact workemail") or "").strip().lower() == email.strip().lower():
                return True
        return False
    except Exception as e:
        print("ch_email_exists error:", e)
        return False

def generate_unique_work_email(first: str, last: str) -> str | None:
    """
    Genera nombre+apellido@dominio y, si ya existe en ChartHop, agrega sufijos numéricos: nombreapellido2@..., nombreapellido3@..., etc.
    """
    base_local = f"{_strip_accents_and_non_alnum(first)}{_strip_accents_and_non_alnum(last)}"
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

def derive_locale_timezone(country_code: str):
    if not country_code:
        return DEFAULT_LOCALE, DEFAULT_TIMEZONE
    cc = country_code.strip().upper()
    locale_map = {
        "MX": "es-MX",
        "CR": "es-CR",
        "CO": "es-CO",
        "AR": "es-AR",
        "CL": "es-CL",
        "US": "en-US",
        "ES": "es-ES",
        "BR": "pt-BR",
    }
    tz_map = {
        "MX": "America/Mexico_City",
        "CR": "America/Costa_Rica",
        "CO": "America/Bogota",
        "AR": "America/Argentina/Buenos_Aires",
        "CL": "America/Santiago",
        "US": "America/Los_Angeles",
        "ES": "Europe/Madrid",
        "BR": "America/Sao_Paulo",
    }
    return locale_map.get(cc, DEFAULT_LOCALE), tz_map.get(cc, DEFAULT_TIMEZONE)

def compose_location(state_or_city: str, country_code: str):
    state = (state_or_city or "").strip()
    cc = (country_code or "").strip()
    if state and cc:
        return f"{state}, {cc}"
    if cc:
        return cc
    return ""


# =========================
# ChartHop helpers
# =========================
def ch_find_job(job_id: str):
    url = f"{CH_API}/v2/org/{CH_ORG_ID}/job"
    params = {"q": f"jobid\\{job_id}", "fields": "title,department name,location name,open"}
    r = requests.get(url, headers=ch_headers(), params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    items = data.get("data") or []
    return items[0] if items else None

def ch_import_people_csv(rows):
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
    r = requests.post(url, headers=ch_headers(), params=params, files=files, timeout=90)
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

    r = requests.post(f"{RUNN_API}/people", headers=runn_headers(), json=payload, timeout=20)
    if r.status_code in (200, 201):
        return r.json()
    if r.status_code == 409 and email:
        q = requests.get(f"{RUNN_API}/people", headers=runn_headers(), params={"email": email}, timeout=20)
        if q.ok and isinstance(q.json(), list) and q.json():
            person = q.json()[0]
            pid = person.get("id")
            requests.patch(f"{RUNN_API}/people/{pid}", headers=runn_headers(), json=payload, timeout=20)
            return {"id": pid}
    r.raise_for_status()

def runn_create_leave(person_id, starts_at, ends_at, reason="Vacation", external_ref=None):
    payload = {"personId": person_id, "startsAt": starts_at, "endsAt": ends_at, "reason": reason}
    if external_ref:
        payload["externalRef"] = external_ref
    r = requests.post(f"{RUNN_API}/time-offs/leave", headers=runn_headers(), json=payload, timeout=20)
    r.raise_for_status()
    return r.json()


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

    # Trae la aplicación y sus relaciones candidate, job
    try:
        app_data = requests.get(
            f"{TT_API}/job-applications/{resource_id}?include=candidate,job",
            headers=tt_headers(),
            timeout=30
        )
    except Exception as e:
        print("TT fetch error:", e)
        return "", 200

    if not app_data.ok:
        return "", 200

    body = app_data.json()
    data = body.get("data", {})
    attributes = data.get("attributes", {}) if isinstance(data, dict) else {}
    status = (attributes.get("status") or attributes.get("state") or "").lower()
    hired_at = attributes.get("hired-at") or attributes.get("hired_at")

    # Solo actuamos si está contratado
    if status != "hired" and not hired_at:
        return "", 200

    included = body.get("included", []) or []
    candidate = next((i for i in included if i.get("type") == "candidates"), {})
    job = next((i for i in included if i.get("type") == "jobs"), {})

    cand_attr = candidate.get("attributes", {}) if isinstance(candidate, dict) else {}
    job_attr = job.get("attributes", {}) if isinstance(job, dict) else {}

    first = cand_attr.get("first-name") or cand_attr.get("first_name") or ""
    last = cand_attr.get("last-name") or cand_attr.get("last_name") or ""
    personal_email = cand_attr.get("email") or ""      # personal de Teamtailor
    title = job_attr.get("title") or ""
    start_date = attributes.get("start-date") or attributes.get("start_date") or (hired_at or "")[:10]

    # Genera correo corporativo único verificando ChartHop
    work_email = generate_unique_work_email(first, last)

    # 1) Upsert en ChartHop: guarda personal email SOLO aquí; y el work email ya definitivo
    rows = [{
        "personal email": personal_email,
        **({"work email": work_email} if work_email else {}),
        "first name": first,
        "last name": last,
        "title": title,
        "start date": start_date
        # agrega "manager work email", "department", etc. si los tienes
    }]
    try:
        ch_import_people_csv(rows)
    except Exception as e:
        print("ChartHop import error:", e)

    # 2) Upsert en Runn usando SOLO correo corporativo
    try:
        full_name = f"{first} {last}".strip()
        if work_email:
            runn_upsert_person(
                name=full_name,
                email=work_email,
                employment_type="employee",
                starts_at=start_date
            )
        else:
            print("Runn upsert skip: no corporate email")
    except Exception as e:
        print("Runn upsert error:", e)

    return "", 200


@app.route("/webhooks/charthop", methods=["POST", "GET"])
def ch_webhook():
    if request.method == "GET":
        return "ChartHop webhook up", 200

    evt = request.get_json(force=True, silent=True) or {}
    evtype = evt.get("type", "")
    entity = evt.get("entitytype", "")
    entity_id = evt.get("entityid")

    # Si se crea un job en ChartHop, abre un Job en Teamtailor como borrador
    if entity == "job" and evtype in ("job.create",):
        job = ch_find_job(str(entity_id))
        if job:
            payload = {
                "data": {
                    "type": "jobs",
                    "attributes": {
                        "title": job.get("title") or "Untitled",
                        "body": "Created from ChartHop",
                        "status": "unlisted"
                    }
                }
            }
            try:
                r = requests.post(f"{TT_API}/jobs", headers=tt_headers(), json=payload, timeout=30)
                print("TT job create status:", r.status_code, r.text[:300])
            except Exception as e:
                print("TT job create error:", e)

    return "", 200


# =========================
# Export nocturno a Culture Amp por SFTP y PTO a Runn
# =========================
def build_ca_csv_from_charthop():
    """
    CSV para Culture Amp con columnas:
    Employee Id, Email, Name, Preferred Name, Manager Email, Location,
    Job Title, Seniority, Locale, Timezone

    Solo usa work email. Si no hay, se omite la fila.
    """
    url = f"{CH_API}/v2/org/{CH_ORG_ID}/job"
    params = {
        "q": "open\\filled",
        "fields": ",".join([
            "jobId",
            "person id",
            "name first",
            "name last",
            "preferred name first",
            "contact workemail",
            "contact personalemail",           # solicitado pero no usado
            "manager contact workemail",
            "title",
            "seniority",
            "homeaddress country",
            "homeaddress region",
        ])
    }
    r = requests.get(url, headers=ch_headers(), params=params, timeout=60)
    r.raise_for_status()
    data = r.json().get("data", [])

    rows = []
    for item in data:
        f = item.get("fields", {}) or {}
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

    columns = ["Employee Id", "Email", "Name", "Preferred Name", "Manager Email",
               "Location", "Job Title", "Seniority", "Locale", "Timezone"]

    sio = io.StringIO()
    writer = csv.DictWriter(sio, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()
    for r_ in rows:
        writer.writerow(r_)
    return sio.getvalue()


def sftp_upload(host, username, password=None, pkey_pem=None, remote_path="/upload/employee_import.csv", content=""):
    key = None
    if pkey_pem:
        key = paramiko.RSAKey.from_private_key(io.StringIO(pkey_pem))
    transport = paramiko.Transport((host, 22))
    if key:
        transport.connect(username=username, pkey=key)
    else:
        transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    with sftp.file(remote_path, "w") as f:
        f.write(content)
    sftp.close()
    transport.close()


@app.route("/cron/nightly", methods=["GET"])
def nightly():
    try:
        csv_text = build_ca_csv_from_charthop()
        fname = f"{CA_SFTP_PATH.rstrip('/')}/employees_{dt.date.today().isoformat()}.csv"
        sftp_upload(
            host=CA_SFTP_HOST, username=CA_SFTP_USER,
            password=CA_SFTP_PASS, pkey_pem=CA_SFTP_KEY,
            remote_path=fname, content=csv_text
        )
    except Exception as e:
        print("Culture Amp SFTP error:", e)

    return "ok", 200


@app.route("/", methods=["GET"])
def health():
    return "OK", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
