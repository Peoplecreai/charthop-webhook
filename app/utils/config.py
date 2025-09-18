import os
import hmac
import hashlib
import base64
import unicodedata
from typing import Tuple


HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "30"))


# =========================
# ChartHop
# =========================
CH_API = os.getenv("CH_API", "https://api.charthop.com")
CH_ORG_ID = os.getenv("CH_ORG_ID")
CH_API_TOKEN = os.getenv("CH_API_TOKEN")
DEFAULT_LOCALE = os.getenv("DEFAULT_LOCALE", "es-LA")
DEFAULT_TIMEZONE = os.getenv("DEFAULT_TIMEZONE", "UTC")
CORP_EMAIL_DOMAIN = os.getenv("CORP_EMAIL_DOMAIN")
AUTO_ASSIGN_WORK_EMAIL = os.getenv("AUTO_ASSIGN_WORK_EMAIL", "false").lower() in ("1", "true", "yes", "on")

# Custom fields para sincronizar IDs cruzados
TT_CF_JOB_CH_ID = os.getenv("TT_CF_JOB_CH_ID")
TT_CF_JOB_CH_API_NAME = os.getenv("TT_CF_JOB_CH_API_NAME", "charthop-job-id")
CH_CF_JOB_TT_ID_LABEL = os.getenv("CH_CF_JOB_TT_ID_LABEL", "teamtailorJobid")


# =========================
# Teamtailor
# =========================
TT_API = os.getenv("TT_API", "https://api.teamtailor.com/v1")
TT_API_KEY = os.getenv("TT_API_KEY") or os.getenv("TEAMTAILOR_API_KEY")
TT_API_VERSION = os.getenv("TT_API_VERSION", "20240404")
TT_SIGNATURE_KEY = os.getenv("TT_SIGNATURE_KEY")


# =========================
# Runn
# =========================
RUNN_API = os.getenv("RUNN_API", "https://api.runn.io")
RUNN_API_TOKEN = os.getenv("RUNN_API_TOKEN")
RUNN_API_VERSION = os.getenv("RUNN_API_VERSION", "1.0.0")
RUNN_CREATE_ON_HIRE = os.getenv("RUNN_CREATE_ON_HIRE", "false").lower() in ("1", "true", "yes", "on")
RUNN_ONBOARDING_LOOKAHEAD_DAYS = int(os.getenv("RUNN_ONBOARDING_LOOKAHEAD_DAYS", "0"))
RUNN_TIMEOFF_LOOKBACK_DAYS = int(os.getenv("RUNN_TIMEOFF_LOOKBACK_DAYS", "7"))
RUNN_TIMEOFF_LOOKAHEAD_DAYS = int(os.getenv("RUNN_TIMEOFF_LOOKAHEAD_DAYS", "30"))


# =========================
# Culture Amp (SFTP)
# =========================
CA_SFTP_HOST = os.getenv("CA_SFTP_HOST")
CA_SFTP_USER = os.getenv("CA_SFTP_USER")
CA_SFTP_PASS = os.getenv("CA_SFTP_PASS")
CA_SFTP_KEY = os.getenv("CA_SFTP_KEY")
CA_SFTP_PASSPHRASE = os.getenv("CA_SFTP_PASSPHRASE")
CA_SFTP_PATH = os.getenv("CA_SFTP_PATH", "/upload")


def ch_headers():
    return {"Authorization": f"Bearer {CH_API_TOKEN}"}


def tt_headers():
    return {
        "Authorization": f"Token token={TT_API_KEY}",
        "X-Api-Version": TT_API_VERSION,
        "Content-Type": "application/vnd.api+json",
    }


def runn_headers():
    return {
        "Authorization": f"Bearer {RUNN_API_TOKEN}",
        "Accept-Version": RUNN_API_VERSION,
        "Content-Type": "application/json",
    }


def tt_verify_signature(resource_id: str, provided_header: str) -> bool:
    if not TT_SIGNATURE_KEY:
        return True
    mac_hex = hmac.new(TT_SIGNATURE_KEY.encode(), resource_id.encode(), hashlib.sha256).hexdigest()
    expected = base64.b64encode(mac_hex.encode()).decode()
    return hmac.compare_digest(provided_header or "", expected)


def strip_accents_and_non_alnum(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", (value or ""))
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    return "".join(ch for ch in ascii_only.lower() if ch.isalnum())


def derive_locale_timezone(country_code: str) -> Tuple[str, str]:
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


def compose_location(state_or_city: str, country_code: str) -> str:
    state = (state_or_city or "").strip()
    cc = (country_code or "").strip()
    if state and cc:
        return f"{state}, {cc}"
    if cc:
        return cc
    return state
