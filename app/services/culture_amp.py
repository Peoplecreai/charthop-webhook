from __future__ import annotations

import datetime as dt

from app.clients.charthop import build_culture_amp_rows, culture_amp_csv_from_rows
from app.clients.sftp import sftp_upload
from app.utils.config import (
    CA_SFTP_HOST,
    CA_SFTP_KEY,
    CA_SFTP_PASS,
    CA_SFTP_PATH,
    CA_SFTP_PASSPHRASE,
    CA_SFTP_USER,
)


def export_culture_amp_snapshot() -> dict:
    rows = build_culture_amp_rows()
    csv_text = culture_amp_csv_from_rows(rows)
    if not csv_text or csv_text.count("\n") <= 1:
        raise RuntimeError("CSV vacío para Culture Amp")
    if not CA_SFTP_HOST or not CA_SFTP_USER:
        raise RuntimeError("Credenciales SFTP incompletas para Culture Amp")
    fname = f"{CA_SFTP_PATH.rstrip('/')}/employees_{dt.date.today().isoformat()}.csv"
    sftp_upload(
        host=CA_SFTP_HOST,
        username=CA_SFTP_USER,
        password=CA_SFTP_PASS,
        pkey_pem=CA_SFTP_KEY,
        passphrase=CA_SFTP_PASSPHRASE,
        remote_path=fname,
        content=csv_text,
    )
    return {"rows": len(rows), "remote_path": fname}
