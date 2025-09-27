from app import create_app

# Usa la factory; las rutas de /tasks/export-runn se montan en app/__init__.py
# (asegúrate de que import_module("app.tasks.runn_export") ocurra ANTES de registrar bp_tasks)
app = create_app()
