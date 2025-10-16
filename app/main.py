from flask import Flask, request

from app.blueprints.charthop_webhook import bp_ch, ch_webhook as ch_handler
from app.blueprints.teamtailor_webhook import bp_tt, tt_webhook as tt_handler
from app.blueprints.cron import bp_cron
from app.tasks.ca_export import bp_tasks  # <-- nuevo
from app.tasks.charthop_worker import bp_charthop_tasks

app = Flask(__name__)
app.register_blueprint(bp_ch)
app.register_blueprint(bp_tt)
app.register_blueprint(bp_cron)
app.register_blueprint(bp_tasks)  # <-- nuevo
app.register_blueprint(bp_charthop_tasks)

@app.route("/health", methods=["GET"])
def health():
    return "OK", 200

@app.route("/", methods=["GET", "POST"])
def root():
    print(f"{request.method} {request.path} len={request.content_length}")
    if request.method == "GET":
        return "OK", 200
    if request.headers.get("Teamtailor-Signature"):
        return tt_handler()
    payload = request.get_json(force=True, silent=True) or {}
    if "resource_id" in payload:
        return tt_handler()
    return ch_handler()

# al final del archivo
if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8080))
    # importante: 0.0.0.0 para que Cloud Run pase el healthcheck
    app.run(host="0.0.0.0", port=port)
