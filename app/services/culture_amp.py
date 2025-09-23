from __future__ import annotations

import os
from typing import Dict

from app.clients.sftp import sftp_upload
from app.utils.config import (
    CA_SFTP_HOST,   # secure.employee-import.integrations.cultureamp.com
    CA_SFTP_USER,   # usuario provisto por CA
    CA_SFTP_KEY,    # llave privada OpenSSH (PEM)
    CH_API,
    CH_ORG_ID,
)
from app.utils.state_gcs import load_state, save_state
from app.clients.charthop import (
    CULTURE_AMP_COLUMNS,
    build_culture_amp_rows,
    culture_amp_csv_from_rows,
    iter_culture_amp_rows_with_ids,
    _row_hash,
    _new_session,
    _get_json,
)


def _upload_csv(text: str) -> None:
    if not text.endswith("\n"):
        text += "\n"
    if not CA_SFTP_HOST or not CA_SFTP_USER or not CA_SFTP_KEY:
        raise RuntimeError("Credenciales SFTP incompletas: host, user y key son obligatorios")
    sftp_upload(
        host=CA_SFTP_HOST,
        username=CA_SFTP_USER,
        pkey_pem=CA_SFTP_KEY,
        remote_path="/employees.csv",
        content=text,
    )


def _full_export() -> dict:
    rows = build_culture_amp_rows()
    csv_text = culture_amp_csv_from_rows(rows)
    if not csv_text or csv_text.count("\n") <= 1:
        raise RuntimeError("CSV vacío para Culture Amp")
    _upload_csv(csv_text)

    # Actualiza manifest con la foto completa (útil para cambiar a delta luego)
    current_meta: Dict[str, dict] = {}
    for r in rows:
        emp_id = r["Employee Id"]
        current_meta[emp_id] = {
            "hash": _row_hash(r),
            "ch_person_id": "",  # no lo tenemos aquí; se poblará en el primer delta
            "row": r,
        }
    save_state({"version": 1, "rows": current_meta})

    return {"rows": len(rows), "remote_path": "/employees.csv", "mode": "full"}


def export_culture_amp_snapshot() -> dict:
    """
    Si CA_EXPORT_MODE=delta => sube solo filas nuevas/cambiadas/terminadas.
    En otro caso => snapshot completo.
    """
    mode = (os.environ.get("CA_EXPORT_MODE") or "full").lower()
    if mode not in {"delta", "full"}:
        mode = "full"

    if mode == "full":
        return _full_export()

    # --------- DELTA ----------
    prev = load_state() or {}
    prev_rows = (prev.get("rows") or {}) if isinstance(prev, dict) else {}

    # Foto actual con ids de CH para poblar manifest
    current: Dict[str, dict] = {}
    current_meta: Dict[str, dict] = {}

    for row, ch_pid in iter_culture_amp_rows_with_ids():
        emp_id = row["Employee Id"]
        current[emp_id] = row
        current_meta[emp_id] = {
            "hash": _row_hash(row),
            "ch_person_id": ch_pid,
            "row": row,
        }

    # nuevos + cambiados
    to_send: Dict[str, dict] = {}
    for emp_id, meta in current_meta.items():
        prev_meta = prev_rows.get(emp_id)
        if not prev_meta:
            to_send[emp_id] = current[emp_id]
            continue
        if meta["hash"] != prev_meta.get("hash"):
            to_send[emp_id] = current[emp_id]

    # faltantes (posibles bajas): estaban antes y ya no están
    missing_ids = set(prev_rows.keys()) - set(current_meta.keys())
    if missing_ids:
        session = _new_session()
        try:
            for emp_id in missing_ids:
                prev_entry = prev_rows.get(emp_id) or {}
                prev_row = dict(prev_entry.get("row") or {})
                end_prev = (prev_row.get("End Date") or "").strip()
                if end_prev:
                    # ya teníamos una fecha de baja en el manifest previo: reenvía esa misma fila
                    to_send[emp_id] = prev_row
                    continue

                ch_pid = (prev_entry.get("ch_person_id") or "").strip()
                if not ch_pid:
                    # sin id de CH no podemos enriquecer: salta
                    continue

                try:
                    url = f"{CH_API}/v2/org/{CH_ORG_ID}/person/{ch_pid}"
                    payload = _get_json(session, url, {"fields": "endDateOrg,contact.workEmail"})
                    if isinstance(payload, dict):
                        end_now = (payload.get("endDateOrg") or "").strip()
                        if end_now:
                            prev_row["End Date"] = end_now[:10]
                            if not prev_row.get("Email"):
                                prev_row["Email"] = (payload.get("contact.workEmail") or "").strip()
                            # Asegura Employee Id por si se perdió
                            if not prev_row.get("Employee Id"):
                                prev_row["Employee Id"] = prev_row.get("Email") or emp_id
                            to_send[emp_id] = prev_row
                except Exception:
                    # ruido de red/permiso; lo dejamos para la próxima corrida
                    pass
        finally:
            session.close()

    if not to_send:
        # Actualiza manifest igualmente con la foto actual
        new_manifest = {
            "version": 1,
            "rows": {
                eid: {
                    "hash": current_meta[eid]["hash"],
                    "ch_person_id": current_meta[eid]["ch_person_id"],
                    "row": current[eid],
                }
                for eid in current.keys()
            },
        }
        save_state(new_manifest)
        return {"rows_sent": 0, "delta": True, "skipped": True}

    # Genera CSV de delta y sube
    csv_text = culture_amp_csv_from_rows(to_send.values())
    if not csv_text or csv_text.count("\n") <= 1:
        # En teoría puede pasar si todos eran líneas vacías; por seguridad no subimos
        return {"rows_sent": 0, "delta": True, "skipped": True}

    _upload_csv(csv_text)

    # Guarda el nuevo manifest (solo actuales; los faltantes desaparecen del estado)
    new_manifest = {
        "version": 1,
        "rows": {
            eid: {
                "hash": current_meta[eid]["hash"],
                "ch_person_id": current_meta[eid]["ch_person_id"],
                "row": current[eid],
            }
            for eid in current.keys()
        },
    }
    save_state(new_manifest)

    return {"rows_sent": len(to_send), "delta": True, "remote_path": "/employees.csv"}
