import os, hmac, hashlib, base64

CH_API = os.getenv("CH_API", "https://api.charthop.com")
CH_ORG_ID = os.getenv("CH_ORG_ID")
CH_API_TOKEN = os.getenv("CH_API_TOKEN")

TT_API = os.getenv("TT_API", "https://api.teamtailor.com/v1")
TT_API_KEY = os.getenv("TT_API_KEY") or os.getenv("TEAMTAILOR_API_KEY")
TT_API_VERSION = os.getenv("TT_API_VERSION", "20240404")
TT_SIGNATURE_KEY = os.getenv("TT_SIGNATURE_KEY")

HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "30"))

TT_CF_JOB_CH_ID = os.getenv("TT_CF_JOB_CH_ID")
TT_CF_JOB_CH_API_NAME = os.getenv("TT_CF_JOB_CH_API_NAME", "charthop-job-id")
CH_CF_JOB_TT_ID_LABEL = os.getenv("CH_CF_JOB_TT_ID_LABEL", "teamtailorJobid")

def ch_headers():
    return {"Authorization": f"Bearer {CH_API_TOKEN}"}

def tt_headers():
    return {"Authorization": f"Token token={TT_API_KEY}",
            "X-Api-Version": TT_API_VERSION,
            "Content-Type": "application/vnd.api+json"}

def tt_verify_signature(resource_id: str, provided_header: str):
    if not TT_SIGNATURE_KEY:
        return True
    mac_hex = hmac.new(TT_SIGNATURE_KEY.encode(), resource_id.encode(), hashlib.sha256).hexdigest()
    expected = base64.b64encode(mac_hex.encode()).decode()
    return hmac.compare_digest(provided_header or "", expected)
