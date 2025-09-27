# app/handlers/runn_full_sync.py
# Handler HTTP o Job entrypoint compatible con RUNN_EXPORT_HANDLER
import os
from app.services.runn_export_all import run_full_sync

def export_handler(request=None):
    # Permite override por query param ?days= o env WINDOW_DAYS
    try:
        days = int(os.getenv("WINDOW_DAYS", "180"))
    except Exception:
        days = 180
    if request is not None:
        try:
            # Flask / Cloud Run request
            arg_days = request.args.get("days")
            if arg_days:
                days = int(arg_days)
        except Exception:
            pass
    run_full_sync(days)
    # Si es HTTP, regresa 200
    return ("ok", 200) if request is not None else None
