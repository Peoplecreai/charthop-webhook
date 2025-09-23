from __future__ import annotations

import csv
from itertools import chain
from typing import Dict, Iterable

from app.clients.charthop import CULTURE_AMP_COLUMNS, iter_culture_amp_rows
from app.clients.sftp import sftp_upload
from app.utils.config import (
    CA_SFTP_HOST,   # secure.employee-import.integrations.cultureamp.com
    CA_SFTP_USER,   # username provisto por CA
    CA_SFTP_KEY,    # llave privada OpenSSH (PEM) desde Secret Manager
)

class _UTF8SFTPWriter:
    def __init__(self, handler):
        self._handler = handler
        self.bytes_written = 0

    def write(self, data: str) -> int:
        if not data:
            return 0
        payload = data.encode("utf-8")
        self._handler.write(payload)
        self.bytes_written += len(payload)
        return len(data)

def _stream_culture_amp_csv(rows: Iterable[Dict[str, str]], handler) -> Dict[str, int]:
    proxy = _UTF8SFTPWriter(handler)
    writer = csv.DictWriter(
        proxy,
        fieldnames=CULTURE_AMP_COLUMNS,
        extrasaction="ignore",
        lineterminator="\n",
    )
    writer.writeheader()
    row_count = 0
    for row in rows:
        writer.writerow(row)
        row_count += 1
    handler.flush()
    return {"rows": row_count, "bytes": proxy.bytes_written}

def export_culture_amp_snapshot() -> dict:
    """
    Genera CSV con headers de Culture Amp y lo sube a la raíz por SFTP.
    Auth con key file. Nombre fijo: /employees.csv
    """
    remote_path = "/employees.csv"

    rows_iter = iter_culture_amp_rows()
    try:
        first_row = next(rows_iter)
    except StopIteration:
        return {"rows": 0, "skipped": True, "remote_path": remote_path}

    if not CA_SFTP_HOST or not CA_SFTP_USER or not CA_SFTP_KEY:
        raise RuntimeError("Credenciales SFTP incompletas: host, user y key son obligatorios")

    stats = sftp_upload(
        host=CA_SFTP_HOST,
        username=CA_SFTP_USER,
        pkey_pem=CA_SFTP_KEY,
        remote_path=remote_path,
        writer=lambda handler: _stream_culture_amp_csv(chain((first_row,), rows_iter), handler),
    ) or {"rows": 0, "bytes": 0}

    return {
        "rows": stats["rows"],
        "bytes": stats.get("bytes", 0),
        "remote_path": remote_path,
    }
