"""
Utilidades para métricas y monitoreo de sincronización.

Permite trackear estadísticas de sincronización para debugging y análisis.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from typing import Any, Dict, Optional

from app.utils.state_gcs import get_state, save_state

logger = logging.getLogger(__name__)

METRICS_STATE_KEY = "sync_metrics.json"


class SyncMetrics:
    """
    Trackea métricas de sincronización.

    Estructura:
    {
        "last_sync": {
            "timeoff": "2025-10-28T12:00:00Z",
            "onboarding": "2025-10-28T11:00:00Z"
        },
        "counters": {
            "timeoff_synced": 150,
            "timeoff_updated": 20,
            "timeoff_deleted": 5,
            "timeoff_skipped": 10,
            "timeoff_errors": 2,
            "person_synced": 45
        },
        "last_errors": [
            {
                "timestamp": "2025-10-28T12:00:00Z",
                "type": "timeoff",
                "error": "person not found",
                "entity_id": "abc123"
            }
        ]
    }
    """

    def __init__(self):
        self._metrics: Dict[str, Any] = self._load_metrics()

    def _load_metrics(self) -> Dict[str, Any]:
        """Carga métricas desde GCS."""
        try:
            data = get_state(METRICS_STATE_KEY)
            if data:
                parsed = json.loads(data) if isinstance(data, str) else data
                return parsed
        except Exception as e:
            logger.warning(f"Could not load sync metrics from GCS: {e}")

        # Estructura por defecto
        return {
            "last_sync": {},
            "counters": {},
            "last_errors": []
        }

    def _save_metrics(self) -> None:
        """Guarda métricas a GCS."""
        try:
            save_state(METRICS_STATE_KEY, json.dumps(self._metrics))
        except Exception as e:
            logger.error(f"Failed to save sync metrics to GCS: {e}")

    def record_sync(self, sync_type: str, timestamp: Optional[str] = None) -> None:
        """
        Registra cuándo ocurrió la última sincronización.

        Args:
            sync_type: Tipo de sync (e.g., "timeoff", "onboarding")
            timestamp: Timestamp ISO 8601 (default: ahora)
        """
        if timestamp is None:
            timestamp = dt.datetime.utcnow().isoformat() + "Z"

        self._metrics["last_sync"][sync_type] = timestamp
        self._save_metrics()

    def increment_counter(self, counter_name: str, amount: int = 1) -> None:
        """
        Incrementa un contador.

        Args:
            counter_name: Nombre del contador
            amount: Cantidad a incrementar
        """
        current = self._metrics["counters"].get(counter_name, 0)
        self._metrics["counters"][counter_name] = current + amount
        self._save_metrics()

    def record_error(
        self,
        error_type: str,
        error_message: str,
        entity_id: Optional[str] = None,
        timestamp: Optional[str] = None
    ) -> None:
        """
        Registra un error.

        Args:
            error_type: Tipo de error (e.g., "timeoff", "person")
            error_message: Mensaje de error
            entity_id: ID de la entidad relacionada (opcional)
            timestamp: Timestamp ISO 8601 (default: ahora)
        """
        if timestamp is None:
            timestamp = dt.datetime.utcnow().isoformat() + "Z"

        error_entry = {
            "timestamp": timestamp,
            "type": error_type,
            "error": error_message,
        }

        if entity_id:
            error_entry["entity_id"] = entity_id

        # Mantener solo los últimos 100 errores
        self._metrics.setdefault("last_errors", [])
        self._metrics["last_errors"].insert(0, error_entry)
        self._metrics["last_errors"] = self._metrics["last_errors"][:100]

        self._save_metrics()

    def get_last_sync(self, sync_type: str) -> Optional[str]:
        """
        Obtiene el timestamp de la última sincronización.

        Args:
            sync_type: Tipo de sync

        Returns:
            Timestamp ISO 8601 o None
        """
        return self._metrics["last_sync"].get(sync_type)

    def get_counter(self, counter_name: str) -> int:
        """
        Obtiene el valor de un contador.

        Args:
            counter_name: Nombre del contador

        Returns:
            Valor del contador
        """
        return self._metrics["counters"].get(counter_name, 0)

    def get_all_counters(self) -> Dict[str, int]:
        """
        Obtiene todos los contadores.

        Returns:
            Diccionario de contadores
        """
        return self._metrics["counters"].copy()

    def get_recent_errors(self, limit: int = 10) -> list[Dict[str, Any]]:
        """
        Obtiene los errores más recientes.

        Args:
            limit: Número máximo de errores a retornar

        Returns:
            Lista de errores
        """
        return self._metrics["last_errors"][:limit]

    def reset_counters(self) -> None:
        """Reinicia todos los contadores."""
        self._metrics["counters"] = {}
        self._save_metrics()
        logger.info("Sync metrics counters reset")


# Instancia singleton
_metrics_instance: Optional[SyncMetrics] = None


def get_sync_metrics() -> SyncMetrics:
    """
    Obtiene la instancia singleton de métricas.

    Returns:
        SyncMetrics instance
    """
    global _metrics_instance
    if _metrics_instance is None:
        _metrics_instance = SyncMetrics()
    return _metrics_instance
