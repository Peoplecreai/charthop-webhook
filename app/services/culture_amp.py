from __future__ import annotations

from app.clients.charthop import build_culture_amp_rows, culture_amp_csv_from_rows
from app.clients.sftp import sftp_upload
from app.utils.config import (
    CA_SFTP_HOST,   # secure.employee-import.integrations.cultureamp.com
    CA_SFTP_USER,   # username provisto por CA
    CA_SFTP_KEY,    # llave privada OpenSSH (PEM) desde Secret Manager
)

def export_culture_amp_snapshot() -> dict:
    """
    Genera CSV con headers de Culture Amp y lo sube a la raíz por SFTP.
    - Auth con key file (sin password).
    - Nombre fijo: /employees.csv
    """
    rows = build_culture_amp_rows()
    csv_text = culture_amp_csv_from_rows(rows)

    if not csv_text or csv_text.count("\n") <= 1:
        raise RuntimeError("CSV vacío para Culture Amp")

    if not csv_text.endswith("\n"):
        csv_text += "\n"

    if not CA_SFTP_HOST or not CA_SFTP_USER or not CA_SFTP_KEY:
        raise RuntimeError("Credenciales SFTP incompletas: host, user y key son obligatorios")

    remote_path = "/employees.csv"

    sftp_upload(
        host=CA_SFTP_HOST,
        username=CA_SFTP_USER,
        pkey_pem=CA_SFTP_KEY,
        remote_path=remote_path,
        content=csv_text,
    )

    return {
        "rows": len(rows),
        "bytes": len(csv_text.encode("utf-8")),
        "remote_path": remote_path,
    }
