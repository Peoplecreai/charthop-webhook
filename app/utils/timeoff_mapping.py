"""
Almacenamiento de mapeo ChartHop timeoff ID <-> Runn timeoff ID.

Usa Google Cloud Storage para persistir el mapeo y poder:
- Actualizar time offs existentes
- Eliminar time offs específicos
- Mantener sincronización idempotente
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

from app.utils.state_gcs import get_state, save_state

logger = logging.getLogger(__name__)

# Nombre del archivo de estado en GCS
TIMEOFF_MAPPING_STATE_KEY = "timeoff_mapping.json"


class TimeoffMapping:
    """
    Maneja el mapeo bidireccional entre IDs de ChartHop y Runn.

    Estructura del mapping:
    {
        "ch_to_runn": {
            "charthop_id": {
                "runn_id": 123,
                "category": "leave",
                "person_email": "user@example.com",
                "created_at": "2025-10-28T12:00:00Z"
            }
        },
        "runn_to_ch": {
            "123": "charthop_id"
        }
    }
    """

    def __init__(self):
        self._mapping: Dict[str, Any] = self._load_mapping()

    def _load_mapping(self) -> Dict[str, Any]:
        """Carga el mapping desde GCS."""
        try:
            data = get_state(TIMEOFF_MAPPING_STATE_KEY)
            if data:
                parsed = json.loads(data) if isinstance(data, str) else data
                return parsed
        except Exception as e:
            logger.warning(f"Could not load timeoff mapping from GCS: {e}")

        # Estructura por defecto
        return {
            "ch_to_runn": {},
            "runn_to_ch": {}
        }

    def _save_mapping(self) -> None:
        """Guarda el mapping a GCS."""
        try:
            save_state(TIMEOFF_MAPPING_STATE_KEY, json.dumps(self._mapping))
        except Exception as e:
            logger.error(f"Failed to save timeoff mapping to GCS: {e}")

    def add(
        self,
        charthop_id: str,
        runn_id: int,
        category: str,
        person_email: str = ""
    ) -> None:
        """
        Agrega un mapeo ChartHop ID -> Runn ID.

        Args:
            charthop_id: ID del time off en ChartHop
            runn_id: ID del time off en Runn
            category: Categoría (leave, holidays, rostered-off)
            person_email: Email de la persona (opcional, para debugging)
        """
        import datetime as dt

        charthop_id = str(charthop_id).strip()
        runn_id = int(runn_id)

        if not charthop_id:
            logger.warning("Cannot add mapping: charthop_id is empty")
            return

        self._mapping["ch_to_runn"][charthop_id] = {
            "runn_id": runn_id,
            "category": category,
            "person_email": person_email,
            "created_at": dt.datetime.utcnow().isoformat() + "Z"
        }

        self._mapping["runn_to_ch"][str(runn_id)] = charthop_id

        self._save_mapping()

        logger.info(
            f"Timeoff mapping added: ChartHop {charthop_id} -> Runn {runn_id} ({category})"
        )

    def get_runn_id(self, charthop_id: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene información del time off en Runn dado el ID de ChartHop.

        Args:
            charthop_id: ID del time off en ChartHop

        Returns:
            Dict con {runn_id, category, person_email, created_at} o None
        """
        charthop_id = str(charthop_id).strip()
        return self._mapping["ch_to_runn"].get(charthop_id)

    def get_charthop_id(self, runn_id: int) -> Optional[str]:
        """
        Obtiene el ID de ChartHop dado el ID de Runn.

        Args:
            runn_id: ID del time off en Runn

        Returns:
            ID de ChartHop o None
        """
        return self._mapping["runn_to_ch"].get(str(runn_id))

    def remove(self, charthop_id: str) -> bool:
        """
        Elimina un mapeo.

        Args:
            charthop_id: ID del time off en ChartHop

        Returns:
            True si se eliminó, False si no existía
        """
        charthop_id = str(charthop_id).strip()

        mapping = self._mapping["ch_to_runn"].get(charthop_id)
        if not mapping:
            return False

        runn_id = str(mapping["runn_id"])

        # Eliminar ambos sentidos del mapeo
        del self._mapping["ch_to_runn"][charthop_id]
        if runn_id in self._mapping["runn_to_ch"]:
            del self._mapping["runn_to_ch"][runn_id]

        self._save_mapping()

        logger.info(f"Timeoff mapping removed: ChartHop {charthop_id}")
        return True

    def get_all_mappings(self) -> Dict[str, Dict[str, Any]]:
        """
        Obtiene todos los mapeos ChartHop -> Runn.

        Returns:
            Diccionario completo de mapeos
        """
        return self._mapping["ch_to_runn"].copy()

    def cleanup_old_mappings(self, days: int = 180) -> int:
        """
        Elimina mapeos más antiguos que X días.

        Args:
            days: Días de antigüedad para limpiar

        Returns:
            Número de mapeos eliminados
        """
        import datetime as dt

        cutoff = dt.datetime.utcnow() - dt.timedelta(days=days)
        to_remove = []

        for ch_id, info in self._mapping["ch_to_runn"].items():
            created_str = info.get("created_at", "")
            try:
                created = dt.datetime.fromisoformat(created_str.replace("Z", "+00:00"))
                if created < cutoff:
                    to_remove.append(ch_id)
            except (ValueError, AttributeError):
                # Si no tiene fecha válida, dejarlo
                continue

        for ch_id in to_remove:
            self.remove(ch_id)

        if to_remove:
            logger.info(f"Cleaned up {len(to_remove)} old timeoff mappings")

        return len(to_remove)


# Instancia singleton
_mapping_instance: Optional[TimeoffMapping] = None


def get_timeoff_mapping() -> TimeoffMapping:
    """
    Obtiene la instancia singleton del mapeo de timeoffs.

    Returns:
        TimeoffMapping instance
    """
    global _mapping_instance
    if _mapping_instance is None:
        _mapping_instance = TimeoffMapping()
    return _mapping_instance
