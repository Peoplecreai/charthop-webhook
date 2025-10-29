from __future__ import annotations
import json
import os
from typing import Dict, Any, Optional

from google.cloud import storage

_BUCKET = os.environ.get("CA_STATE_BUCKET", "")
_MISSING = object()

def _client() -> storage.Client:
    return storage.Client()


def get_state(object_path: str) -> Optional[Any]:
    """
    Lee un archivo de estado desde GCS.
    
    Args:
        object_path: Ruta del objeto en el bucket (ej: "timeoff_mapping.json")
    
    Returns:
        Contenido del archivo (dict o string) o None si no existe
    """
    if not _BUCKET:
        return None
    
    try:
        cli = _client()
        bkt = cli.bucket(_BUCKET)
        blob = bkt.blob(object_path)
        
        if not blob.exists():
            return None
        
        data = blob.download_as_text(encoding="utf-8")
        
        # Intentar parsear como JSON
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            return data
    except Exception as e:
        print(f"Error loading state from GCS ({object_path}): {e}")
        return None


def save_state(object_path: Any, data: Any = _MISSING) -> None:
    """
    Guarda datos en GCS.

    Args:
        object_path: Ruta del objeto en el bucket
        data: Datos a guardar (dict, list, o string)
    """
    if data is _MISSING:
        data = object_path
        object_path = os.environ.get("CA_STATE_OBJECT", "culture-amp/state.json")
    else:
        object_path = str(object_path)

    if not _BUCKET:
        print(f"Warning: CA_STATE_BUCKET not set, cannot save state to {object_path}")
        return

    try:
        cli = _client()
        bkt = cli.bucket(_BUCKET)
        blob = bkt.blob(object_path)

        # Convertir a string si es necesario
        if isinstance(data, (dict, list)):
            content = json.dumps(
                data,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
        else:
            content = str(data)

        blob.upload_from_string(
            content.encode("utf-8"),
            content_type="application/json; charset=utf-8",
        )
    except Exception as e:
        print(f"Error saving state to GCS ({object_path}): {e}")


# Mantener compatibilidad con código existente de Culture Amp
def load_state() -> Dict[str, Any]:
    """
    Carga el estado legacy de Culture Amp.
    Mantiene compatibilidad con el código existente.
    """
    object_path = os.environ.get("CA_STATE_OBJECT", "culture-amp/state.json")
    result = get_state(object_path)
    return result if isinstance(result, dict) else {}
