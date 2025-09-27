"""Request dispatch helpers for the root webhook endpoint."""

from __future__ import annotations

import logging
from typing import Callable

from flask import Request
from flask.typing import ResponseReturnValue

from app.blueprints.charthop_webhook import ch_webhook
from app.blueprints.teamtailor_webhook import tt_webhook

logger = logging.getLogger(__name__)

Handler = Callable[[], ResponseReturnValue]


def build_root_response(req: Request | None) -> ResponseReturnValue:
    """Route incoming requests to the appropriate webhook handler."""

    if req is None:
        logger.warning("Received root request without an active request object")
        return "OK", 200

    logger.info(
        "%s %s len=%s headers=%s",
        req.method,
        req.path,
        req.content_length,
        {k: v for k, v in req.headers.items() if k.lower().startswith("teamtailor")},
    )

    if req.method == "GET":
        return "OK", 200

    handler = _resolve_handler(req)
    return handler()


def _resolve_handler(req: Request) -> Handler:
    if _is_teamtailor_request(req):
        logger.debug("Dispatching request to Teamtailor webhook handler")
        return tt_webhook

    logger.debug("Dispatching request to ChartHop webhook handler")
    return ch_webhook


def _is_teamtailor_request(req: Request) -> bool:
    if req.headers.get("Teamtailor-Signature"):
        return True
    payload = req.get_json(force=True, silent=True) or {}
    return "resource_id" in payload

