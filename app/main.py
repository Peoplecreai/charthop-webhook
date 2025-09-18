from flask import Flask, request
from app.blueprints.charthop_webhook import bp_ch
from app.blueprints.teamtailor_webhook import bp_tt

app = Flask(__name__)
app.register_blueprint(bp_ch)
app.register_blueprint(bp_tt)

@app.route("/health", methods=["GET"])
def health():
    return "OK", 200

@app.route("/", methods=["GET", "POST"])
def root():
    print(f"{request.method} {request.path} len={request.content_length}")
    if request.method == "GET":
        return "OK", 200
    # Multiplexor simple: si viene cabecera de TT o resource_id, pásalo a TT webhook
    if request.headers.get("Teamtailor-Signature"):
        return bp_tt.view_functions["teamtailor_webhook"]()
    payload = request.get_json(force=True, silent=True) or {}
    if "resource_id" in (payload or {}):
        return bp_tt.view_functions["teamtailor_webhook"]()
    return bp_ch.view_functions["charthop_webhook"]()
