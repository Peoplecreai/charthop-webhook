"""Compatibility package for legacy import paths.

This package re-exports handlers implemented under the :mod:`app.handlers`
namespace so existing integrations that import from ``handlers`` continue to
work without modification.
"""
from app.handlers.runn_to_bq import export_handler as runn_export_handler

__all__ = ["runn_export_handler"]
