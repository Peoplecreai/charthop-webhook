# app/__init__.py
from importlib import import_module
from flask import Flask

# Blueprints principales
from app.blueprints.charthop_webhook import bp_ch
from app.blueprints.teamtailor_webhook import bp_tt
from app.blueprints.cron import bp_cron
from app.tasks.ca_export import bp_tasks  # Blueprint de /tasks

def create_app() -> Flask:
    app = Flask(__name__)

    # Importa módulos que añaden rutas al blueprint de tareas
    # Esto DEBE ocurrir antes de registrar bp_tasks en la app.
    import_module("app.tasks.runn_export")  # define /tasks/export-runn en bp_tasks

    # Registra blueprints
    app.register_blueprint(bp_ch)
    app.register_blueprint(bp_tt)
    app.register_blueprint(bp_cron)
    app.register_blueprint(bp_tasks)

    @app.get("/health")
    def health():
        return "OK", 200

    return app
