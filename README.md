# ChartHop Webhook Service

Servicio Flask que actúa como concentrador de webhooks entre ChartHop, Teamtailor y varios procesos de sincronización internos.

## Arquitectura

- **Application factory (`app/__init__.py`)**: encapsula la creación de la aplicación Flask, el registro de *blueprints* y las rutas básicas (`/` y `/health`). Esto facilita las pruebas y evita efectos colaterales al importar módulos.
- **Blueprints**:
  - `app.blueprints.charthop_webhook`: recibe eventos de ChartHop relacionados con puestos y delega en los servicios de sincronización.
  - `app.blueprints.teamtailor_webhook`: valida la firma de Teamtailor y procesa contrataciones.
  - `app.blueprints.cron`: expone endpoints usados por Cloud Scheduler para ejecutar sincronizaciones periódicas.
  - `app.tasks.ca_export.bp_tasks`: agrupa las tareas que se ejecutan en segundo plano mediante Cloud Tasks (Culture Amp y Runn).
- **Handlers y servicios**: la lógica de negocio vive en `app/services/*` y `app/handlers/*`. Por ejemplo, `app/handlers/root.py` decide a qué webhook delegar las peticiones entrantes y `app/services/culture_amp.py` construye los archivos de exportación.
- **Clientes externos** (`app/clients/*`): encapsulan las llamadas HTTP a ChartHop, Teamtailor, Runn y servicios auxiliares (SFTP, GCS, etc.).
- **Utilidades** (`app/utils/*`): funciones de configuración, manejo de estado en GCS y helpers compartidos.

La nueva factoría de aplicación configura el *logging* estándar para que los mensajes de depuración y errores queden centralizados y reemplaza los `print()` sueltos por llamadas al módulo `logging`.

## Requisitos

- Python 3.10+
- Dependencias indicadas en `requirements.txt`
- Credenciales para los servicios externos (ChartHop, Teamtailor, Runn, Culture Amp, Google Cloud)

Instalación rápida:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Ejecución local

```bash
export FLASK_APP=app.main:app
flask run --debug
```

También puedes usar `gunicorn` (el módulo `main.py` en la raíz expone la aplicación como `from app.main import app`).

## Variables de entorno principales

| Grupo | Variable | Descripción |
|-------|----------|-------------|
| ChartHop | `CH_API`, `CH_ORG_ID`, `CH_API_TOKEN`, `CH_PEOPLE_PAGE_SIZE` | Parámetros de API y paginación. |
| Teamtailor | `TT_API_KEY`, `TT_SIGNATURE_KEY`, `TT_API_VERSION`, `TT_CF_JOB_CH_API_NAME` | Acceso a la API y validación de firmas. |
| Runn | `RUNN_API_TOKEN`, `RUNN_API_VERSION`, `RUNN_ONBOARDING_LOOKAHEAD_DAYS`, `RUNN_TIMEOFF_LOOKBACK_DAYS` | Sincronización de onboarding y ausencias. |
| Culture Amp | `CA_SFTP_HOST`, `CA_SFTP_USER`, `CA_SFTP_KEY`, `CA_EXPORT_MODE` | Exportación de datos hacia SFTP. |
| Cloud Tasks / Cloud Run | `GCP_PROJECT`, `TASKS_LOCATION`, `TASKS_QUEUE`, `RUN_SERVICE_URL`, `TASKS_SA_EMAIL` | Configuración para encolar tareas asíncronas. |

Consulta `app/utils/config.py` y `app/tasks/ca_export.py` para el detalle completo.

## Endpoints relevantes

| Endpoint | Método | Descripción |
|----------|--------|-------------|
| `/` | `GET` | Respuesta `OK` (health light). |
| `/` | `POST` | Entrada unificada de webhooks. El *dispatcher* analiza cabeceras/payload y delega en Teamtailor o ChartHop automáticamente. |
| `/health` | `GET` | Health check básico. |
| `/webhooks/charthop` | `GET/POST` | Endpoint específico de ChartHop. |
| `/webhooks/teamtailor` | `POST` | Endpoint específico de Teamtailor con validación HMAC. |
| `/cron/nightly` | `GET/POST` | Encola la exportación a Culture Amp. |
| `/cron/runn/onboarding` | `GET/POST` | Sincroniza onboardings con Runn. |
| `/cron/runn/timeoff` | `GET/POST` | Sincroniza ausencias con Runn. |
| `/tasks/export-culture-amp` | `POST` | Ejecuta la exportación (invocada por Cloud Tasks). |
| `/tasks/export-runn` | `POST` | Exportación de Runn a BigQuery (si el módulo opcional está disponible). |

## Desarrollo y pruebas

- El proyecto carece de pruebas automatizadas; se recomienda aislar la lógica de negocio (servicios) para facilitar la creación de tests en el futuro.
- Los *blueprints* ahora son registrados dentro de la factoría `create_app`, por lo que en pruebas basta con importar y llamar a esa función para obtener una instancia limpia de la app.
- Para depurar, usa la variable `FLASK_ENV=development` o el flag `--debug` y revisa los logs estructurados (nivel INFO por defecto).

## Despliegue

- `main.py` (en la raíz) expone el objeto `app` para servidores como Gunicorn o Cloud Run.
- Asegúrate de configurar las variables de entorno obligatorias antes de desplegar.
- Las tareas asíncronas requieren el paquete `google-cloud-tasks` instalado en el entorno.

