"""Compatibility wrapper for the Runn to BigQuery export handler.

Historically the project exposed the export handler as ``handlers.runn_to_bq``.
The implementation now lives in :mod:`app.handlers.runn_to_bq`, so this module
simply proxies calls to the new location.  Keeping this thin wrapper ensures
that any external invocation (for example Google Cloud Functions) that still
imports ``handlers.runn_to_bq`` continues to function without code changes.
"""
from __future__ import annotations

from typing import Any

from app.handlers.runn_to_bq import export_handler as _export_handler

__all__ = ["export_handler"]


def export_handler(*args: Any, **kwargs: Any) -> Any:
    """Delegate to :func:`app.handlers.runn_to_bq.export_handler`.

    Parameters
    ----------
    *args: Any
        Positional arguments forwarded to the underlying handler.
    **kwargs: Any
        Keyword arguments forwarded to the underlying handler.

    Returns
    -------
    Any
        The result of :func:`app.handlers.runn_to_bq.export_handler`.
    """

    return _export_handler(*args, **kwargs)
