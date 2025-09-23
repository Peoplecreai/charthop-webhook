from __future__ import annotations
import json
import os
from typing import Dict, Any, Optional
from google.cloud import storage

_BUCKET = os.environ.get("CA_STATE_BUCKET", "")
_OBJECT = os.environ.get("CA_STATE_OBJECT", "culture-amp/state.json")

def _client() -> storage.Client:
    return storage.Client()

def load_state() -> Dict[str, Any]:
    """Devuelve { rows: {empId: {hash, ch_person_id, row}}, version: 1 } o vacío."""
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
    if not _BUCKET or not _OBJECT:
        return
    cli = _client()
    bkt = cli.bucket(_BUCKET)
    blob = bkt.blob(_OBJECT)
    payload = json.dumps(state, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    blob.upload_from_string(payload, content_type="application/json; charset=utf-8")
