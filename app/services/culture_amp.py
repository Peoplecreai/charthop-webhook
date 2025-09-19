from __future__ import annotations

from app.clients.charthop import build_culture_amp_rows, culture_amp_csv_from_rows
from app.clients.sftp import sftp_upload
from app.utils.config import (
    CA_SFTP_HOST,   # "secure.employee-import.integrations.cultureamp.com"
    CA_SFTP_KEY,    # llave privada OpenSSH (PEM) asociada al usuario en CA
    CA_SFTP_USER,   # ej. "creai"
)


def export_culture_amp_snapshot() -> dict:
    """
    Exporta snapshot de empleados y lo sube a Culture Amp por SFTP.
    - Autenticación: SOLO Key File (OpenSSH). No password.
    - Directorio: raíz "/" (no se pueden crear carpetas).
    - Operación: PUT del archivo final (no rename/move).
    - Formato: CSV UTF-8, encabezados exactos.
    """
    rows = build_culture_amp_rows()
    csv_text = culture_amp_csv_from_rows(rows)

    # Si no hay filas, no falles el cron: marca 'skipped'
    if not csv_text or csv_text.count("\n") <= 1:
        return {"rows": 0, "skipped": True, "remote_path": "/employees.csv"}

    # Asegura newline final (parsers quisquillosos)
    if not csv_text.endswith("\n"):
        csv_text += "\n"

    # Validación de credenciales obligatorias
    if not CA_SFTP_HOST or not CA_SFTP_USER or not CA_SFTP_KEY:
        raise RuntimeError("Credenciales SFTP incompletas: host, user y key son obligatorios")

    # Nombre fijo en la raíz
    remote_path = "/employees.csv"

    # Subida por SFTP usando únicamente key auth (content como str; sftp_upload codifica)
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
        "skipped": False,
    }
