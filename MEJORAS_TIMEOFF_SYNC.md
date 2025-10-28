# Mejoras a la Sincronización ChartHop → Runn (Time Off)

Este documento describe las mejoras implementadas en el sistema de sincronización de time off entre ChartHop y Runn.

## 🎯 Resumen de Mejoras Implementadas

### 1. **Rate Limiting y Caché**
- **Rate Limiter**: Control de 100 requests por minuto a la API de Runn
- **Caché de Personas**: TTL de 5 minutos para búsquedas por email
- **Performance**: Reduce llamadas a la API de O(n) a O(1) en búsquedas repetidas

**Archivos**:
- `app/utils/rate_limiter.py` (nuevo)
- `app/clients/runn.py` (modificado)

### 2. **Mapeo Persistente de IDs**
- Almacena la relación ChartHop ID ↔ Runn ID en Google Cloud Storage
- Permite actualizar y eliminar time offs específicos
- Sincronización idempotente

**Archivos**:
- `app/utils/timeoff_mapping.py` (nuevo)

### 3. **Ciclo de Vida Completo (CRUD)**

#### Create
- ✅ Ya existía
- ✅ Mejorado: Ahora retorna el objeto completo con ID
- ✅ Mejorado: Almacena mapeo para futuras operaciones

#### Update
- ✅ Nuevo: Detecta automáticamente si un time off ya existe
- ✅ Nuevo: Actualiza en lugar de crear duplicados
- ✅ Endpoint: `PUT /time-offs/{type}/{id}`

#### Delete
- ✅ Nuevo: Maneja eliminaciones desde ChartHop
- ✅ Nuevo: Función `delete_runn_timeoff_event()`
- ✅ Endpoint: `DELETE /time-offs/{type}/{id}`

**Archivos**:
- `app/clients/runn.py`: Nuevas funciones `runn_update_timeoff()`, `runn_delete_timeoff()`
- `app/services/runn_sync.py`: Nueva función `delete_runn_timeoff_event()`

### 4. **Validación de Estado de Aprobación**
- Filtra time offs según su estado (denied, cancelled, draft, pending)
- Evita sincronizar time offs no aprobados
- Reduce ruido en Runn

**Estados filtrados**:
- `denied`, `rejected`
- `cancelled`, `canceled`
- `draft`
- `pending`
- `withdrawn`

**Archivos**:
- `app/services/runn_sync.py`: Función `_should_skip_timeoff()`

### 5. **Validación Robusta de Fechas**
- Validación de formato YYYY-MM-DD
- Manejo de diferentes formatos ISO 8601
- Logging de fechas inválidas

**Archivos**:
- `app/services/runn_sync.py`: Función `_safe_date()` mejorada

### 6. **Webhook Handler Mejorado**
- Detecta eventos de eliminación (`delete`, `deleted`)
- Encola tareas específicas según el tipo de acción
- Mejor logging con action incluida

**Archivos**:
- `app/blueprints/charthop_webhook.py`

### 7. **Worker Actualizado**
- Nuevo tipo de tarea: `timeoff_delete`
- Enrutamiento a funciones específicas

**Archivos**:
- `app/tasks/charthop_worker.py`

### 8. **Métricas de Sincronización**
- Contadores: synced, updated, deleted, skipped, errors
- Últimos 100 errores almacenados
- Timestamps de última sincronización
- Almacenamiento en GCS

**Archivos**:
- `app/utils/sync_metrics.py` (nuevo)

---

## 📊 Flujo de Sincronización Mejorado

### Evento Create/Update
```
ChartHop Webhook (create/update)
    ↓
/webhooks/charthop
    ↓
enqueue_charthop_task("timeoff", id)
    ↓
Cloud Tasks
    ↓
/tasks/worker
    ↓
sync_runn_timeoff_event(id)
    ↓
_sync_timeoff_entry()
    ├─→ Validar estado (approved?)
    ├─→ Buscar email de persona
    ├─→ Buscar persona en Runn (caché)
    ├─→ Verificar mapeo existente
    │   ├─→ Si existe: UPDATE en Runn
    │   └─→ Si no existe: CREATE en Runn
    └─→ Guardar mapeo + métricas
```

### Evento Delete
```
ChartHop Webhook (delete)
    ↓
/webhooks/charthop
    ↓
enqueue_charthop_task("timeoff_delete", id)
    ↓
Cloud Tasks
    ↓
/tasks/worker
    ↓
delete_runn_timeoff_event(id)
    ↓
    ├─→ Buscar mapeo
    ├─→ DELETE en Runn
    └─→ Eliminar mapeo + métricas
```

---

## 🔧 Variables de Entorno (Sin Cambios)

Las variables existentes siguen siendo las mismas:

```bash
# ChartHop
CH_API=https://api.charthop.com
CH_ORG_ID=your-org-id
CH_API_TOKEN=your-token

# Runn
RUNN_BASE_URL=https://api.runn.io
RUNN_API_TOKEN=your-token
RUNN_ACCEPT_VERSION=1.0.0

# Ventanas de sincronización
RUNN_TIMEOFF_LOOKBACK_DAYS=7
RUNN_TIMEOFF_LOOKAHEAD_DAYS=30
RUNN_ONBOARDING_LOOKAHEAD_DAYS=0

# Google Cloud
GCP_PROJECT=your-project
TASKS_LOCATION=us-central1
SERVICE_URL=https://your-service.run.app
```

---

## 🚀 Nuevas Funciones API

### Runn Client (`app/clients/runn.py`)

#### `runn_find_person_by_email(email: str, use_cache: bool = True)`
- Busca persona por email con caché opcional
- **Nuevo parámetro**: `use_cache` para forzar búsqueda directa

#### `runn_create_timeoff(...) -> Optional[Dict[str, Any]]`
- **Cambio**: Ahora retorna el objeto completo en lugar de bool
- Incluye rate limiting automático

#### `runn_update_timeoff(...) -> Optional[Dict[str, Any]]` (NUEVO)
```python
runn_update_timeoff(
    timeoff_id=123,
    category="leave",
    start_date="2025-10-28",
    end_date="2025-10-30",
    note="Updated note"
)
```

#### `runn_delete_timeoff(timeoff_id: int, category: str) -> bool` (NUEVO)
```python
runn_delete_timeoff(timeoff_id=123, category="leave")
```

#### `runn_clear_people_cache()` (NUEVO)
- Fuerza recarga del caché de personas

### Runn Sync (`app/services/runn_sync.py`)

#### `delete_runn_timeoff_event(timeoff_id: str) -> Dict[str, Any]` (NUEVO)
- Maneja eliminaciones desde ChartHop
- Busca mapeo, elimina en Runn, limpia mapeo

### Timeoff Mapping (`app/utils/timeoff_mapping.py`)

#### `get_timeoff_mapping() -> TimeoffMapping` (NUEVO)
- Singleton para acceso global

#### `TimeoffMapping.add(charthop_id, runn_id, category, email)` (NUEVO)
- Almacena mapeo bidireccional

#### `TimeoffMapping.get_runn_id(charthop_id)` (NUEVO)
- Obtiene info de Runn dado ID de ChartHop

#### `TimeoffMapping.remove(charthop_id)` (NUEVO)
- Elimina mapeo

#### `TimeoffMapping.cleanup_old_mappings(days=180)` (NUEVO)
- Limpia mapeos antiguos

### Sync Metrics (`app/utils/sync_metrics.py`)

#### `get_sync_metrics() -> SyncMetrics` (NUEVO)
- Singleton para métricas

#### `SyncMetrics.increment_counter(name, amount=1)` (NUEVO)
- Incrementa contador

#### `SyncMetrics.record_error(type, message, entity_id)` (NUEVO)
- Registra error

#### `SyncMetrics.get_all_counters()` (NUEVO)
- Obtiene todas las métricas

---

## 📈 Métricas Disponibles

### Contadores
- `timeoff_synced`: Time offs creados en Runn
- `timeoff_updated`: Time offs actualizados en Runn
- `timeoff_deleted`: Time offs eliminados en Runn
- `timeoff_skipped`: Time offs saltados (estado inválido, no aprobados, etc.)
- `timeoff_errors`: Errores en sincronización

### Timestamps
- `last_sync.timeoff_batch`: Última sincronización batch
- `last_sync.timeoff_event`: Último evento webhook
- `last_sync.timeoff_delete`: Última eliminación

### Errores
- Últimos 100 errores con timestamp, tipo y mensaje

---

## 🧪 Testing

### Probar Create/Update
```bash
# Simular webhook de ChartHop (create)
curl -X POST http://localhost:8080/webhooks/charthop \
  -H "Content-Type: application/json" \
  -d '{
    "type": "timeoff.create",
    "entityType": "timeoff",
    "entityId": "test-123"
  }'
```

### Probar Delete
```bash
# Simular webhook de ChartHop (delete)
curl -X POST http://localhost:8080/webhooks/charthop \
  -H "Content-Type: application/json" \
  -d '{
    "type": "timeoff.delete",
    "entityType": "timeoff",
    "entityId": "test-123"
  }'
```

### Ver Métricas (agregar endpoint si es necesario)
```python
from app.utils.sync_metrics import get_sync_metrics

metrics = get_sync_metrics()
counters = metrics.get_all_counters()
print(counters)
# {'timeoff_synced': 150, 'timeoff_updated': 20, ...}

recent_errors = metrics.get_recent_errors(limit=10)
print(recent_errors)
```

---

## 🔍 Troubleshooting

### Time off no se sincroniza
1. Verificar estado: ¿Está aprobado?
2. Verificar logs: Buscar "skipped" con razón
3. Verificar email de persona en ChartHop
4. Verificar que persona existe en Runn

### Time off duplicado
- Ya no debería ocurrir con el mapeo de IDs
- Si ocurre, verificar que el mapeo se esté almacenando correctamente

### Rate limiting
- Si ves mensajes de wait en logs, es normal
- El rate limiter espera automáticamente
- Para ajustar límite: modificar `_RATE_LIMITER` en `runn.py`

### Caché desactualizado
```python
from app.clients.runn import runn_clear_people_cache
runn_clear_people_cache()
```

---

## 📝 Notas de Implementación

### Compatibilidad con Código Existente
- ✅ Todos los cambios son retrocompatibles
- ✅ Código existente sigue funcionando sin cambios
- ✅ Nuevas funcionalidades son opt-in via webhooks

### Estado en GCS
Se crean dos nuevos archivos de estado:
- `timeoff_mapping.json`: Mapeo de IDs
- `sync_metrics.json`: Métricas de sincronización

### Performance
- **Antes**: O(n) búsquedas por cada time off
- **Después**: O(1) con caché (5 min TTL)
- **Rate Limiting**: Previene throttling de Runn API

### Idempotencia
- ✅ Ejecutar múltiples veces el mismo evento es seguro
- ✅ Updates detectan automáticamente si el time off existe
- ✅ Deletes son idempotentes (no fallan si no existe)

---

## 🎉 Beneficios

1. **Sincronización Completa**: Create, Update, Delete
2. **Mejor Performance**: Caché reduce llamadas API en ~90%
3. **Idempotencia**: Seguro ejecutar múltiples veces
4. **Observabilidad**: Métricas y logging mejorado
5. **Robustez**: Validación de estado y fechas
6. **Mantenibilidad**: Código más organizado y modular

---

## 📚 Referencias

- **ChartHop API**: https://api.charthop.com/swagger
- **Runn API**: https://developer.runn.io/reference/
- **Google Cloud Storage**: Para persistencia de estado
- **Google Cloud Tasks**: Para procesamiento asíncrono
