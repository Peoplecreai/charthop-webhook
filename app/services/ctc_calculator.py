from __future__ import annotations

import logging
from typing import Any, Dict, List

from app.clients.charthop import (
    ch_get_person_compensation,
    ch_iter_people_v2,
    ch_update_job_ctc,
)
from app.utils.sync_metrics import get_sync_metrics

logger = logging.getLogger(__name__)


def _calculate_ctc_from_formula(
    base_comp: float, esquema_contratacion: str
) -> float:
    """
    Calcula el Cost to Company como: salario anual + fee según esquema.
    """
    if base_comp <= 0:
        return 0.0

    esquema = (esquema_contratacion or "").strip().lower()

    # Fee según esquema de contratación (país)
    fee = 0.0
    if esquema == "voiz":
        fee = 240.0
    elif esquema == "ontop":
        fee = 720.0

    # CTC = Salario anual + Fee
    total_ctc = base_comp + fee
    return round(total_ctc, 2)


def calculate_and_update_ch_ctc(person_id: str) -> Dict[str, Any]:
    """
    Proceso para una sola persona:
    1. Obtiene datos de ChartHop (Base, Empleo, País, JobId).
    2. Calcula el nuevo CTC usando la fórmula.
    3. Escribe el nuevo CTC de vuelta en el Job de ChartHop.
    """
    metrics = get_sync_metrics()
    
    # 1. Obtener datos
    comp_data = ch_get_person_compensation(person_id)

    if not comp_data:
        metrics.increment_counter("ctc_calc_skipped")
        return {"status": "skipped", "reason": "person_not_found", "person_id": person_id}

    job_id = comp_data.get("job_id")
    base_comp = comp_data.get("base_comp", 0.0)
    esquema_contratacion = comp_data.get("esquema_contratacion")
    currency = comp_data.get("currency", "USD")

    if not job_id:
        metrics.increment_counter("ctc_calc_skipped")
        return {"status": "skipped", "reason": "missing_job_id", "person_id": person_id}
    
    if base_comp <= 0:
        metrics.increment_counter("ctc_calc_skipped")
        return {"status": "skipped", "reason": "missing_base_comp", "person_id": person_id, "job_id": job_id}

    # 2. Calcular
    new_ctc = _calculate_ctc_from_formula(base_comp, esquema_contratacion)

    if new_ctc <= 0:
        metrics.increment_counter("ctc_calc_skipped")
        return {"status": "skipped", "reason": "calculation_is_zero", "person_id": person_id, "job_id": job_id}

    try:
        # 3. Escribir de vuelta en ChartHop
        # El CTC siempre debe estar en USD, independientemente de la moneda del base
        ch_update_job_ctc(job_id, new_ctc, "USD")

        metrics.increment_counter("ctc_calc_updated")
        metrics.record_sync("ctc_calc_event")
        
        return {
            "status": "updated_charthop",
            "person_id": person_id,
            "job_id": job_id,
            "new_ctc": new_ctc,
            "base_comp": base_comp,
            "esquema_contratacion": esquema_contratacion,
        }
    except Exception as e:
        logger.error(
            f"Failed to update ChartHop CTC for person {person_id} / job {job_id}",
            exc_info=e
        )
        metrics.increment_counter("ctc_calc_errors")
        metrics.record_error("ctc_calc", str(e), person_id)
        return {
            "status": "error",
            "reason": "failed_to_write_charthop",
            "person_id": person_id,
            "job_id": job_id,
            "error": str(e),
        }


def batch_calculate_and_update_ch_ctc() -> Dict[str, Any]:
    """
    Proceso batch para "full runn":
    Itera todas las personas activas y ejecuta el cálculo para cada una.
    """
    metrics = get_sync_metrics()
    
    # Usamos ch_iter_people_v2 para obtener solo IDs de personas activas
    people_iter = ch_iter_people_v2("id")
    
    results: List[Dict[str, Any]] = []
    processed = 0

    for person in people_iter:
        person_id = (person.get("id") or "").strip()
        if not person_id:
            continue
        
        processed += 1
        result = calculate_and_update_ch_ctc(person_id)
        results.append(result)

    summary = {
        "processed": processed,
        "updated": sum(1 for r in results if r.get("status") == "updated_charthop"),
        "skipped": sum(1 for r in results if r.get("status") == "skipped"),
        "error": sum(1 for r in results if r.get("status") == "error"),
        "results": results,
    }
    
    metrics.record_sync("ctc_calc_batch")
    logger.info("CTC calculation batch summary", extra=summary)
    return summary
