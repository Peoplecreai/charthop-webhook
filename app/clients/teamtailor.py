import requests
from app.utils.config import TT_API, tt_headers, TT_CF_JOB_CH_ID, TT_CF_JOB_CH_API_NAME, HTTP_TIMEOUT

def tt_create_job_from_ch(title: str, body: str = "Created from ChartHop", status: str = "unlisted"):
    payload = {"data": {"type": "jobs", "attributes": {"title": title or "Untitled", "body": body, "status": status}}}
    r = requests.post(f"{TT_API}/jobs", headers=tt_headers(), json=payload, timeout=HTTP_TIMEOUT)
    return r

def tt_get_custom_field_id_by_api_name(api_name: str) -> str | None:
    r = requests.get(f"{TT_API}/custom-fields", headers=tt_headers(),
                     params={"filter[api-name]": api_name}, timeout=HTTP_TIMEOUT)
    if not r.ok:
        return None
    for cf in (r.json() or {}).get("data", []):
        attrs = cf.get("attributes") or {}
        if (attrs.get("api-name") or attrs.get("api_name")) == api_name:
            return cf.get("id")
    return None

def tt_find_job_custom_field_value_id(job_id: str, custom_field_id: str) -> str | None:
    r = requests.get(f"{TT_API}/jobs/{job_id}", headers=tt_headers(),
                     params={"include": "custom-field-values,custom-field-values.custom-field"},
                     timeout=HTTP_TIMEOUT)
    if not r.ok:
        return None
    inc = (r.json() or {}).get("included") or []
    for item in inc:
        if item.get("type") == "custom-field-values":
            rel = (item.get("relationships") or {}).get("custom-field") or {}
            if str((rel.get("data") or {}).get("id")) == str(custom_field_id):
                return item.get("id")
    return None

def tt_upsert_job_custom_field(job_id: str, value: str,
                               custom_field_id: str | None = None,
                               api_name: str | None = TT_CF_JOB_CH_API_NAME):
    cf_id = custom_field_id or TT_CF_JOB_CH_ID or tt_get_custom_field_id_by_api_name(api_name)
    if not cf_id:
        raise RuntimeError("No se pudo resolver el ID del custom field de Teamtailor")

    # crear
    payload = {"data":{"type":"custom-field-values","attributes":{"value":str(value)},
              "relationships":{"owner":{"data":{"type":"jobs","id":str(job_id)}},
                               "custom-field":{"data":{"type":"custom-fields","id":str(cf_id)}}}}}
    url = f"{TT_API}/custom-field-values"
    r = requests.post(url, headers=tt_headers(), json=payload, timeout=HTTP_TIMEOUT)
    if r.status_code in (200, 201):
        return r.json()

    # actualizar si ya existe
    cfv_id = tt_find_job_custom_field_value_id(job_id, cf_id)
    if cfv_id:
        patch = {"data":{"id":cfv_id,"type":"custom-field-values","attributes":{"value":str(value)}}}
        pr = requests.patch(f"{TT_API}/custom-field-values/{cfv_id}", headers=tt_headers(), json=patch, timeout=HTTP_TIMEOUT)
        pr.raise_for_status()
        return pr.json()
    r.raise_for_status()

def tt_fetch_application(app_id: str):
    return requests.get(f"{TT_API}/job-applications/{app_id}",
                        headers=tt_headers(),
                        params={"include":"candidate,job,offers"},
                        timeout=HTTP_TIMEOUT)

def tt_get_offer_start_date_for_application(app_id: str) -> str | None:
    try:
        r = tt_fetch_application(app_id)
        if r.ok:
            payload = r.json() or {}
            for inc in (payload.get("included") or []):
                if inc.get("type") in ("job-offers", "offers"):
                    attrs = inc.get("attributes") or {}
                    details = attrs.get("details") or {}
                    sd = (details.get("start-date") or details.get("start_date") or "").strip()
                    if sd:
                        return sd[:10]
            # relación offers si viene con links
            data = payload.get("data") or {}
            rels = (data.get("relationships") or {})
            links = (rels.get("offers") or rels.get("job-offers") or {}).get("links") or {}
            if links.get("related"):
                rr = requests.get(links["related"], headers=tt_headers(), timeout=HTTP_TIMEOUT)
                if rr.ok:
                    body = rr.json() or {}
                    items = body.get("data")
                    items = items if isinstance(items, list) else [items]
                    for of in items or []:
                        attrs = of.get("attributes") or {}
                        details = attrs.get("details") or {}
                        sd = (details.get("start-date") or details.get("start_date") or "").strip()
                        if sd:
                            return sd[:10]
        # último intento: colección filtrada
        rr = requests.get(f"{TT_API}/job-offers", headers=tt_headers(),
                          params={"filter[job-application-id]": app_id}, timeout=HTTP_TIMEOUT)
        if rr.ok:
            for of in (rr.json() or {}).get("data", []):
                attrs = of.get("attributes") or {}
                details = attrs.get("details") or {}
                sd = (details.get("start-date") or details.get("start_date") or "").strip()
                if sd:
                    return sd[:10]
    except Exception as e:
        print("tt_get_offer_start_date_for_application error:", repr(e))
    return None

