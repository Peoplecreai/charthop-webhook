from __future__ import annotations

import datetime as dt

from app.clients.charthop import build_culture_amp_rows, culture_amp_csv_from_rows
from app.clients.sftp import sftp_upload
from app.utils.config import (
    CA_SFTP_HOST,   # p. ej. "secure.employee-import.integrations.cultureamp.com"
    CA_SFTP_KEY,    # llave privada OpenSSH en texto PEM
    CA_SFTP_USER,   # p. ej. "creai"
)


def export_culture_amp_snapshot() -> dict:
    """
    Exporta el snapshot de empleados para Culture Amp y lo sube por SFTP.
    Requisitos según CA:
    - Autenticación únicamente con llave (Key File).
    - Directorio de carga en la raíz "/".
    - Solo operación PUT. No se puede renombrar ni mover.
    - CSV UTF-8 con encabezados exactos.
    """
    rows = build_culture_amp_rows()
    csv_text = culture_amp_csv_from_rows(rows)

    if not csv_text or csv_text.count("\n") <= 1:
        raise RuntimeError("CSV vacío para Culture Amp")

    # Asegurar newline final
    if not csv_text.endswith("\n"):
        csv_text += "\n"

    # Validación de credenciales obligatorias
    if not CA_SFTP_HOST or not CA_SFTP_USER or not CA_SFTP_KEY:
        raise RuntimeError("Credenciales SFTP incompletas: host, user y key son obligatorios")

    # Nombre fijo en raíz
    remote_path = "/employees.csv"

    # Subida por SFTP usando únicamente key auth
    sftp_upload(
        host=CA_SFTP_HOST,
        username=CA_SFTP_USER,
        pkey_pem=CA_SFTP_KEY,           # llave privada OpenSSH
        remote_path=remote_path,
        content=csv_text.encode("utf-8")  # UTF-8
    )

    return {
        "rows": len(rows),
        "bytes": len(csv_text.encode("utf-8")),
        "remote_path": remote_path,
    }
