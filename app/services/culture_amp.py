from __future__ import annotations

import datetime as dt

from app.clients.charthop import build_culture_amp_rows, culture_amp_csv_from_rows
from app.clients.sftp import sftp_upload
from app.utils.config import (
    CA_SFTP_HOST,
    CA_SFTP_KEY,         # OpenSSH private key (requerido por CA)
    CA_SFTP_PASS,        # opcional; CA prioriza key auth
    CA_SFTP_PATH,        # ignoraremos subcarpetas; usaremos "/"
    CA_SFTP_PASSPHRASE,  # si tu key tiene passphrase
    CA_SFTP_USER,
)


def export_culture_amp_snapshot() -> dict:
    rows = build_culture_amp_rows()
    csv_text = culture_amp_csv_from_rows(rows)

    # Validación de contenido
    if not csv_text or csv_text.count("\n") <= 1:
        raise RuntimeError("CSV vacío para Culture Amp")

    # Asegurar newline final (algunos parsers lo requieren)
    if not csv_text.endswith("\n"):
        csv_text += "\n"

    # Requisitos de conexión SFTP según CA: host, user y clave SSH (OpenSSH)
    if not CA_SFTP_HOST or not CA_SFTP_USER or not CA_SFTP_KEY:
        raise RuntimeError("Credenciales SFTP incompletas: host/user/key son obligatorios para Culture Amp")

    # Culture Amp recomienda el directorio raíz "/" (y solo permite PUT)
    # Evitamos depender de subcarpetas configuradas.
    remote_dir = "/"
    fname = f"employees_{dt.date.today().isoformat()}.csv"
    remote_path = f"{remote_dir}{fname}"

    # Subir en UTF-8
    sftp_upload(
        host=CA_SFTP_HOST,
        username=CA_SFTP_USER,
        password=CA_SFTP_PASS,     # opcional; clave SSH es la principal
        pkey_pem=CA_SFTP_KEY,      # OpenSSH key
        passphrase=CA_SFTP_PASSPHRASE,
        remote_path=remote_path,
        content=csv_text.encode("utf-8"),
    )

    return {"rows": len(rows), "remote_path": remote_path, "bytes": len(csv_text.encode("utf-8"))}
