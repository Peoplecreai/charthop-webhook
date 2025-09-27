"""Application factory and bootstrap helpers for the webhook service."""

from __future__ import annotations

import logging
from importlib import import_module
from typing import Callable

from flask import Flask, request
from flask.typing import ResponseReturnValue

from app.blueprints.charthop_webhook import bp_ch
from app.blueprints.cron import bp_cron
from app.blueprints.teamtailor_webhook import bp_tt
from app.handlers.root import build_root_response
from app.tasks.ca_export import bp_tasks

BlueprintRegistrar = Callable[[Flask], None]

__all__ = ["create_app"]


def create_app(*, register: BlueprintRegistrar | None = None) -> Flask:
    """Create and configure the Flask application instance.

    Parameters
    ----------
    register:
        Optional hook that allows tests to register additional blueprints
        without mutating global state. When omitted, the default application
        blueprints are registered.
    """

    app = Flask(__name__)
    _configure_logging(app)

    if register is None:
        register = _register_default_blueprints

    register(app)
    _register_core_routes(app)
    return app


def _configure_logging(app: Flask) -> None:
    """Ensure the Flask logger is configured with a sensible default."""

    if not app.logger.handlers:
        logging.basicConfig(level=logging.INFO)
    else:
        for handler in app.logger.handlers:
            handler.setLevel(logging.INFO)


def _register_default_blueprints(app: Flask) -> None:
      # Importa las rutas que se montan sobre bp_tasks ANTES de registrar el blueprint
  import_module("app.tasks.runn_export")

    app.register_blueprint(bp_ch)
    app.register_blueprint(bp_tt)
    app.register_blueprint(bp_cron)
    app.register_blueprint(bp_tasks)


def _register_core_routes(app: Flask) -> None:
    @app.get("/health")
    def health() -> ResponseReturnValue:
        return "OK", 200

    @app.route("/", methods=["GET", "POST"])
    def root() -> ResponseReturnValue:
        return build_root_response(request)

