from __future__ import annotations
import json
import logging
import os
from typing import Dict, Any, Optional

from google.cloud import storage

logger = logging.getLogger(__name__)

_BUCKET = os.environ.get("CA_STATE_BUCKET", "")
_OBJECT = os.environ.get("CA_STATE_OBJECT", "culture-amp/state.json")


def _client() -> storage.Client:
    return storage.Client()


def load_state() -> Dict[str, Any]:
    """
    Devuelve el manifest previo:
    {
      "version": 1,
      "rows": {
         "<Employee Id>": {
            "hash": "<sha256>",
            "ch_person_id": "<ChartHop person id>",
            "row": {<fila CSV como dict>}
         },
         ...
      }
    }
    o {} si no existe.
    """
    if not _BUCKET or not _OBJECT:
        return {}
    cli = _client()
    bkt = cli.bucket(_BUCKET)
    blob = bkt.blob(_OBJECT)
    if not blob.exists():
        return {}
    data = blob.download_as_text(encoding="utf-8")
    try:
        parsed = json.loads(data)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    return {}


def save_state(state: Dict[str, Any]) -> None:
    """Guarda el manifest actual en GCS."""
    if not _BUCKET or not _OBJECT:
        return
    cli = _client()
    bkt = cli.bucket(_BUCKET)
    blob = bkt.blob(_OBJECT)
    payload = json.dumps(state, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    blob.upload_from_string(payload, content_type="application/json; charset=utf-8")


def get_state(key: str) -> Optional[str]:
    """
    Obtiene el contenido de un archivo de estado específico desde GCS.

    Args:
        key: Nombre del archivo/objeto en el bucket (ej: "timeoff_mapping.json")

    Returns:
        Contenido del archivo como string, o None si no existe o falla
    """
    if not _BUCKET:
        logger.debug(f"CA_STATE_BUCKET not configured, skipping get_state for {key}")
        return None

    try:
        cli = _client()
        bkt = cli.bucket(_BUCKET)
        # Usar el key directamente como ruta del blob
        blob = bkt.blob(key)

        if not blob.exists():
            logger.debug(f"State file does not exist: {key}")
            return None

        data = blob.download_as_text(encoding="utf-8")
        return data
    except Exception as e:
        logger.warning(f"Failed to get state from GCS for {key}: {e}")
        return None


def save_state_keyed(key: str, data: str) -> None:
    """
    Guarda contenido en un archivo de estado específico en GCS.

    Args:
        key: Nombre del archivo/objeto en el bucket (ej: "timeoff_mapping.json")
        data: Contenido a guardar (string, típicamente JSON)
    """
    if not _BUCKET:
        logger.debug(f"CA_STATE_BUCKET not configured, skipping save for {key}")
        return

    try:
        cli = _client()
        bkt = cli.bucket(_BUCKET)
        blob = bkt.blob(key)
        blob.upload_from_string(data, content_type="application/json; charset=utf-8")
        logger.debug(f"State saved to GCS: {key}")
    except Exception as e:
        logger.error(f"Failed to save state to GCS for {key}: {e}")
