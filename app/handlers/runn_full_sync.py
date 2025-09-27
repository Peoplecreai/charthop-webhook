# app/handlers/runn_full_sync.py
# Handler/orquestador simple para ejecutar el Full Sync desde CLI o Gunicorn.

import os
import logging

from app.services.runn_export_all import run_full_sync

logging.basicConfig(level=logging.INFO)

def export_handler(_event=None):
    """
    Entry point compatible con tu env var RUNN_EXPORT_HANDLER = app.handlers.runn_full_sync:export_handler
    """
    window = os.getenv("WINDOW_DAYS")
    wd = int(window) if window else None
    run_full_sync(window_days=wd)

if __name__ == "__main__":
    export_handler(None)
