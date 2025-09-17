import os, io, csv, hmac, hashlib, base64, datetime as dt, unicodedata, socket
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
TT_API_VERSION = os.getenv("TT_API_VERSION", "20240404")
TT_SIGNATURE_KEY = os.getenv("TT_SIGNATURE_KEY")

RUNN_API = os.getenv("RUNN_API", "https://api.runn.io")
RUNN_API_TOKEN = os.getenv("RUNN_API_TOKEN")
RUNN_API_VERSION = os.getenv("RUNN_API_VERSION", "1.0.0")

CA_SFTP_HOST = os.getenv("CA_SFTP_HOST")
CA_SFTP_USER = os.getenv("CA_SFTP_USER")
CA_SFTP_PASS = os.getenv("CA_SFTP_PASS")
CA_SFTP_KEY = os.getenv("CA_SFTP_KEY")
CA_SFTP_PASSPHRASE = os.getenv("CA_SFTP_PASSPHRASE")
CA_SFTP_PATH = os.getenv("CA_SFTP_PATH", "/upload")

# Custom fields para enlazar vacantes
TT_CF_JOB_CH_ID = os.getenv("TT_CF_JOB_CH_ID")  # id del custom field en Teamtailor
TT_CF_JOB_CH_API_NAME = os.getenv("TT_CF_JOB_CH_API_NAME", "charthop-job-id")
CH_CF_JOB_TT_ID_LABEL = os.getenv("CH_CF_JOB_TT_ID_LABEL", "teamtailorJobid")  # etiqueta visible en CH

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
    # HMAC-SHA256(resource_id) -> hex -> base64, si TT_SIGNATURE_KEY está seteada
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

def _evt_get(d: dict, *keys):
    """Obtiene d[key] tolerando casing y variantes."""
    if not isinstance(d, dict):
        return None
    for k in keys:
        if k in d:
            return d[k]
    dl = { (k or "").lower(): v for k, v in d.items() }
    for k in keys:
        v = dl.get((k or "").lower())
        if v is not None:
            return v
    return None

def _has_key(d: dict, *keys) -> bool:
    if not isinstance(d, dict):
        return False
    lower = { (k or "").lower() for k in d.keys() }
    return any(((k in d) or (k.lower() in lower)) for k in keys)

# =========================
# ChartHop helpers
# =========================
def ch_find_job(job_id: str):
    """Busca un Job por jobid usando el endpoint y q=jobid\{id} (doc oficial)."""
    url = f"{CH_API}/v2/org/{CH_ORG_ID}/job"
    params = {
        "q": f"jobid\\{job_id}",  # ChartHop carrot filter: field\value
        "fields": "title,department name,location name,open"
    }
    try:
        r = requests.get(url, headers=ch_headers(), params=params, timeout=HTTP_TIMEOUT)
        if r.status_code == 200:
            items = (r.json() or {}).get("data") or []
            return items[0] if items else None
        # log útil para diagnosticar permisos/sintaxis
        print("ch_find_job status:", r.status_code, (r.text or "")[:300])
    except Exception as e:
        print("ch_find_job error:", e)
    return None

def ch_email_exists(email: str) -> bool:
    if not email: return False
    try:
        url = f"{CH_API}/v2/org/{CH_ORG_ID}/person"
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

def ch_upsert_job_field(job_id: str, field_label: str, value: str):
    """Actualiza un campo custom de Job en CH vía import CSV."""
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

# =========================
# Teamtailor custom fields helpers
# =========================
def tt_get_custom_field_id_by_api_name(api_name: str) -> str | None:
    r = requests.get(f"{TT_API}/custom-fields", headers=tt_headers(),
                     params={"filter[api-name]": api_name}, timeout=HTTP_TIMEOUT)
    if r.ok:
        data = (r.json() or {}).get("data") or []
        for cf in data:
            attrs = cf.get("attributes") or {}
            if (attrs.get("api-name") or attrs.get("api_name")) == api_name:
                return cf.get("id")
    return None

def tt_find_job_custom_field_value_id(job_id: str, custom_field_id: str) -> str | None:
    r = requests.get(f"{TT_API}/jobs/{job_id}",
                     headers=tt_headers(),
                     params={"include": "custom-field-values,custom-field-values.custom-field"},
                     timeout=HTTP_TIMEOUT)
    if not r.ok:
        return None
    inc = (r.json() or {}).get("included") or []
    for item in inc:
        if item.get("type") == "custom-field-values":
            rel = (item.get("relationships") or {}).get("custom-field") or {}
            rel_data = rel.get("data") or {}
            if str(rel_data.get("id")) == str(custom_field_id):
                return item.get("id")
    return None

def tt_upsert_job_custom_field(job_id: str, value: str,
                               custom_field_id: str | None = None,
                               api_name: str | None = TT_CF_JOB_CH_API_NAME):
    cf_id = custom_field_id or TT_CF_JOB_CH_ID
    if not cf_id and api_name:
        cf_id = tt_get_custom_field_id_by_api_name(api_name)
    if not cf_id:
        raise RuntimeError("No se pudo resolver el ID del custom field de Teamtailor")

    payload = {
        "data": {
            "type": "custom-field-values",
            "attributes": {"value": str(value)},
            "relationships": {
                "owner": {"data": {"type": "jobs", "id": str(job_id)}},
                "custom-field": {"data": {"type": "custom-fields", "id": str(cf_id)}}
            }
        }
    }
    url = f"{TT_API}/custom-field-values"
    r = requests.post(url, headers=tt_headers(), json=payload, timeout=HTTP_TIMEOUT)
    if r.status_code in (200, 201):
        return r.json()

    cfv_id = tt_find_job_custom_field_value_id(job_id, cf_id)
    if cfv_id:
        patch = {
            "data": {
                "id": cfv_id,
                "type": "custom-field-values",
                "attributes": {"value": str(value)}
            }
        }
        pr = requests.patch(f"{TT_API}/custom-field-values/{cfv_id}",
                            headers=tt_headers(), json=patch, timeout=HTTP_TIMEOUT)
        pr.raise_for_status()
        return pr.json()

    r.raise_for_status()

def tt_get_job_charthop_id(tt_job_id: str) -> str | None:
    """Lee el valor del custom field charthop_job_id en un Job de TT."""
    cf_id = TT_CF_JOB_CH_ID or tt_get_custom_field_id_by_api_name(TT_CF_JOB_CH_API_NAME)
    if not cf_id:
        return None
    r = requests.get(f"{TT_API}/jobs/{tt_job_id}",
                     headers=tt_headers(),
                     params={"include": "custom-field-values,custom-field-values.custom-field"},
                     timeout=HTTP_TIMEOUT)
    if not r.ok:
        return None
    inc = (r.json() or {}).get("included") or []
    for item in inc:
        if item.get("type") == "custom-field-values":
            rel = (item.get("relationships") or {}).get("custom-field") or {}
            if str((rel.get("data") or {}).get("id")) == str(cf_id):
                return (item.get("attributes") or {}).get("value")
    return None

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
    payload = {"personId": person_id, "startsAt": starts_at, "EndsAt": ends_at, "reason": reason}
    if external_ref: payload["externalRef"] = external_ref
    r = requests.post(f"{RUNN_API}/time-offs/leave", headers=runn_headers(), json=payload, timeout=HTTP_TIMEOUT)
    r.raise_for_status(); return r.json()

# =========================
# Export nocturno a Culture Amp
# =========================
def build_ca_csv_from_charthop():
    url = f"{CH_API}/v2/org/{CH_ORG_ID}/person"
    fields = ",".join([
        "person id","name first","name last","preferred name first",
        "contact workemail","contact personalemail",
        "manager contact workemail","title","seniority",
        "homeaddress country","homeaddress region","status",
    ])
    rows, limit, offset = [], 200, 0

    while True:
        params = {"fields": fields, "limit": limit, "offset": offset}
        r = requests.get(url, headers=ch_headers(), params=params, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        data = r.json() or {}
        items = data.get("data") or []
        if not items:
            break

        for it in items:
            f = it.get("fields") or {}
            status = (f.get("status") or "").strip().lower()
            if status and status not in ("active", "current", "enabled"):
                continue

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
        offset += len(items)

    cols = ["Employee Id","Email","Name","Preferred Name","Manager Email",
            "Location","Job Title","Seniority","Locale","Timezone"]
    sio = io.StringIO()
    w = csv.DictWriter(sio, fieldnames=cols, extrasaction="ignore")
    w.writeheader()
    for rw in rows:
        w.writerow(rw)
    return sio.getvalue()

# =========================
# SFTP con timeouts y soporte Ed25519/RSA
# =========================
def _sftp_ensure_dirs(sftp: paramiko.SFTPClient, remote_dir: str):
    if not remote_dir or remote_dir == "/":
        return
    parts = []
    for p in remote_dir.strip("/").split("/"):
        parts.append(p)
        path = "/" + "/".join(parts)
        try:
            sftp.stat(path)
        except FileNotFoundError:
            sftp.mkdir(path)

def sftp_upload(host: str, username: str, password: str = None, pkey_pem: str = None,
                remote_path: str = "/upload/employee_import.csv", content: str = "") -> None:
    sock = socket.create_connection((host, 22), timeout=15)
    transport = paramiko.Transport(sock)
    transport.banner_timeout = 15

    if pkey_pem:
        buf = io.StringIO(pkey_pem)
        key = None
        try:
            key = paramiko.Ed25519Key.from_private_key(buf, password=CA_SFTP_PASSPHRASE)
        except Exception:
            buf.seek(0)
            key = paramiko.RSAKey.from_private_key(buf, password=CA_SFTP_PASSPHRASE)
        transport.connect(username=username, pkey=key)
    else:
        if not password:
            raise RuntimeError("SFTP necesita CA_SFTP_KEY o CA_SFTP_PASS")
        transport.connect(username=username, password=password)

    sftp = paramiko.SFTPClient.from_transport(transport)
    try:
        remote_dir = os.path.dirname(remote_path) or "/"
        _sftp_ensure_dirs(sftp, remote_dir)
        with sftp.file(remote_path, "wb") as f:
            f.write(content.encode("utf-8"))
            f.flush()
    finally:
        try:
            sftp.close()
        finally:
            transport.close()

# =========================
# Webhooks
# =========================
@app.route("/webhooks/charthop", methods=["POST", "GET"])
def ch_webhook():
    if request.method == "GET":
        return "ChartHop webhook up", 200

    evt = request.get_json(force=True, silent=True) or {}
    evtype = _evt_get(evt, "type", "eventType", "event_type") or ""
    entity = _evt_get(evt, "entityType", "entitytype", "entity_type") or ""
    entity_id = str(_evt_get(evt, "entityId", "entityid", "entity_id") or "")

    print(f"CH evt: type={evtype} entity={entity} entity_id={entity_id}")

    # normalizaciones
    is_job    = entity.lower() in ("job", "jobs")
    is_create = evtype.lower() in ("job.create", "job_create", "create")
    is_update = evtype.lower() in ("job.update", "job_update", "update", "change")

    if is_job and is_create:
        try:
            if not entity_id:
                print("CH job create: missing entity_id")
                return "", 200

            # 1) Buscar el job en CH (puede dar 404/401 -> lo capturamos)
            job = ch_find_job(entity_id)
            if not job:
                print(f"CH job create: job {entity_id} not found in CH")
                return "", 200

            # 2) Crear job en TT
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
            r = requests.post(f"{TT_API}/jobs", headers=tt_headers(), json=payload, timeout=HTTP_TIMEOUT)
            print("TT job create status:", r.status_code, str(r.text)[:300])
            r.raise_for_status()

            # 3) Leer id y enlazar IDs
            tt_job_json = r.json() or {}
            tt_job_id = ((tt_job_json.get("data") or {}).get("id") or "").strip()
            if tt_job_id:
                try:
                    tt_upsert_job_custom_field(tt_job_id, entity_id)  # TT: charthop-job-id
                except Exception as e:
                    print("TT set charthop_job_id error:", e)
                try:
                    ch_upsert_job_field(entity_id, CH_CF_JOB_TT_ID_LABEL, tt_job_id)  # CH: teamtailorJobid
                except Exception as e:
                    print("CH set teamtailorJobid error:", e)

        except Exception as e:
            # Capturamos TODO para no devolver 500 a ChartHop
            print("CH job create handler error:", repr(e))

        return "", 200

    elif is_job and is_update:
        print(f"CH job update ignored (entity_id={entity_id})")
        return "", 200

    # Otros eventos CH
    return "", 200

# =========================
# Multiplexor raíz
# =========================
@app.route("/", methods=["GET", "POST"])
def root():
    print(f"{request.method} {request.path} UA={request.headers.get('User-Agent')} len={request.content_length}")
    if request.method == "GET":
        return "OK", 200
    payload = request.get_json(force=True, silent=True) or {}
    # Si parece Teamtailor
    if request.headers.get("Teamtailor-Signature") or _has_key(payload, "resource_id"):
        return tt_webhook()
    # Por defecto, ChartHop
    return ch_webhook()

# =========================
# Cron nocturno: CSV -> Culture Amp por SFTP
# =========================
@app.route("/cron/nightly", methods=["GET"])
def nightly():
    try:
        csv_text = build_ca_csv_from_charthop()
        if not csv_text or csv_text.count("\n") <= 1:
            raise RuntimeError("CSV vacío para Culture Amp")
        print("CA CSV bytes:", len(csv_text.encode("utf-8")))
        fname = f"{CA_SFTP_PATH.rstrip('/')}/employees_{dt.date.today().isoformat()}.csv"
        sftp_upload(
            host=CA_SFTP_HOST,
            username=CA_SFTP_USER,
            password=CA_SFTP_PASS,
            pkey_pem=CA_SFTP_KEY,
            remote_path=fname,
            content=csv_text
        )
    except Exception as e:
        print("Culture Amp SFTP error:", e)
    return "ok", 200

@app.route("/health", methods=["GET"])
def health():
    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
