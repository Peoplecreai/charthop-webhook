# 🔧 Correcciones para tus Errores Específicos

## Error 1: 405 Method Not Allowed ✅ RESUELTO

**Tu comando:**
```bash
curl -i -H "Authorization: Bearer $ID_TOKEN" "$SERVICE_URL/tasks/worker"
```

**Problema:** Falta `-X POST`

**✅ Corrección:**
```bash
curl -i -X POST -H "Authorization: Bearer $ID_TOKEN" "$SERVICE_URL/tasks/worker"
```

---

## Error 2: 503 Service Unavailable ❌ REQUIERE ACCIÓN

**Tu comando:**
```bash
curl -i -X POST "$SERVICE_URL/tasks/worker" \
  -H "Authorization: Bearer $ID_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "kind": "time_off.created",  # ← Este también está mal
        "entity_id": "person-1234",
        ...
      }'
```

**Problema:** El servicio no se ha redeployado con el nuevo código

**✅ Solución:**

### Opción 1: Si tienes CI/CD configurado
```bash
# Push el código
git push origin main

# Esperar ~5 minutos y verificar
gcloud run services describe charthop-webhook \
  --region northamerica-south1 \
  --format='value(status.latestReadyRevisionName)'
```

### Opción 2: Deploy manual
```bash
# Build
gcloud builds submit --tag gcr.io/integration-hub-468417/charthop-webhook

# Deploy
gcloud run deploy charthop-webhook \
  --image gcr.io/integration-hub-468417/charthop-webhook \
  --region northamerica-south1 \
  --platform managed
```

**Además, tu payload está mal. Corrección:**

```bash
curl -i -X POST "$SERVICE_URL/tasks/worker" \
  -H "Authorization: Bearer $ID_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "kind": "timeoff",           # ← Cambio: sin punto ni "created"
        "entity_id": "67235d4eb7e4c100191a36bd"  # ← ID real de ChartHop
      }'
```

**Nota:** El worker espera un `entity_id` que sea un **ID real de timeoff en ChartHop**, no `person-1234`. También elimina el campo `payload` que pusiste, no es necesario.

---

## Error 3: jq error con Runn API ❌ REQUIERE CORRECCIÓN

**Tu comando:**
```bash
curl -sS -H "Authorization: Bearer $RUNN_API_TOKEN" \
     -H "Accept-Version: $RUNN_ACCEPT_VERSION" \
     "$RUNN_BASE_URL/people" \
| jq --arg e "$EMAIL" -r '.[] | select((.email // "") | ascii_downcase == ($e|ascii_downcase)) | {id,name,email}'
```

**Error:**
```
jq: error (at <stdin>:0): Cannot index array with string "email"
```

**Problema:** La respuesta de Runn no es un array simple de objetos con `email` directo.

**✅ Corrección:**

```bash
EMAIL="lilianaecheverri@creai.mx"

# Primero, ver la estructura real
curl -sS -H "Authorization: Bearer $RUNN_API_TOKEN" \
     -H "Accept-Version: $RUNN_ACCEPT_VERSION" \
     "$RUNN_BASE_URL/people" \
| jq '.[0:2]'  # Ver los primeros 2 registros

# Luego buscar por email (ajustado según la estructura real)
curl -sS -H "Authorization: Bearer $RUNN_API_TOKEN" \
     -H "Accept-Version: $RUNN_ACCEPT_VERSION" \
     "$RUNN_BASE_URL/people" \
| jq --arg e "$EMAIL" -r '
  .[]
  | select(.email != null and (.email | ascii_downcase) == ($e | ascii_downcase))
  | {id: .id, name: .name, email: .email}
'
```

**Alternativa más simple:**
```bash
# Buscar sin case-insensitive primero
curl -sS -H "Authorization: Bearer $RUNN_API_TOKEN" \
     -H "Accept-Version: $RUNN_ACCEPT_VERSION" \
     "$RUNN_BASE_URL/people" \
| jq --arg e "$EMAIL" '.[] | select(.email == $e)'
```

---

## 📝 Resumen de Comandos Corregidos

### 1. Test del Worker (CORRECTO)

```bash
# Configurar variables
SERVICE_URL="$(gcloud run services describe charthop-webhook \
  --region northamerica-south1 --format='value(status.address.url)')"

SA="$(gcloud run services describe charthop-webhook \
  --region northamerica-south1 \
  --format='value(spec.template.spec.serviceAccountName)')"

ID_TOKEN="$(gcloud auth print-identity-token \
  --impersonate-service-account="$SA" \
  --audiences="$SERVICE_URL")"

# Test con un ID real de ChartHop
curl -i -X POST "$SERVICE_URL/tasks/worker" \
  -H "Authorization: Bearer $ID_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "kind": "timeoff",
    "entity_id": "67235d4eb7e4c100191a36bd"
  }'
```

### 2. Test del Webhook (CORRECTO)

```bash
# Test CREATE
curl -X POST "$SERVICE_URL/webhooks/charthop" \
  -H "Content-Type: application/json" \
  -d '{
    "type": "timeoff.create",
    "entityType": "timeoff",
    "entityId": "test-123"
  }'

# Test DELETE
curl -X POST "$SERVICE_URL/webhooks/charthop" \
  -H "Content-Type: application/json" \
  -d '{
    "type": "timeoff.delete",
    "entityType": "timeoff",
    "entityId": "test-123"
  }'
```

### 3. Buscar persona en Runn (CORRECTO)

```bash
export RUNN_BASE_URL="https://api.runn.io"
export RUNN_ACCEPT_VERSION="1.0.0"
export RUNN_API_TOKEN="$(gcloud secrets versions access latest \
  --secret=RUNN_API_TOKEN \
  --project=integration-hub-468417)"

EMAIL="lilianaecheverri@creai.mx"

curl -sS -H "Authorization: Bearer $RUNN_API_TOKEN" \
     -H "Accept-Version: $RUNN_ACCEPT_VERSION" \
     "$RUNN_BASE_URL/people" \
| jq --arg email "$EMAIL" '
  .[]
  | select(.email != null and (.email | ascii_downcase) == ($email | ascii_downcase))
'
```

---

## ⚡ Quick Fix: Usa el Script de Test

En lugar de ejecutar comandos manualmente, usa el script que creé:

```bash
# Dar permisos
chmod +x test_webhook.sh

# Ejecutar tests automáticos
./test_webhook.sh
```

Este script:
- ✅ Usa los comandos correctos
- ✅ Valida las respuestas
- ✅ Muestra errores en color
- ✅ Incluye múltiples tests

---

## 🎯 Checklist de lo que necesitas hacer AHORA

- [ ] **1. Redeploy del servicio** (ver "Error 2" arriba)
- [ ] **2. Esperar 5 minutos** a que el deploy termine
- [ ] **3. Ejecutar** `./test_webhook.sh`
- [ ] **4. Verificar** que no hay errores 503
- [ ] **5. Si hay errores**, revisar logs:
  ```bash
  gcloud logging read \
    'resource.type="cloud_run_revision"
     AND resource.labels.service_name="charthop-webhook"
     AND severity>=ERROR' \
    --project=integration-hub-468417 \
    --freshness=10m \
    --limit=20
  ```

---

## 🆘 Si el Redeploy falla

```bash
# Ver qué está fallando
gcloud builds list --limit=5

# Ver logs del último build
gcloud builds log $(gcloud builds list --limit=1 --format='value(id)')
```

---

## ✅ Cuando todo funcione

Deberías ver:

```bash
./test_webhook.sh

# Output esperado:
✓ SERVICE_URL: https://charthop-webhook-xxx.run.app
✓ Service Account: 878406902076-compute@...
✓ Token obtenido
✓ Webhook recibido correctamente (200)
✓ Webhook DELETE recibido correctamente (200)
✓ Worker ejecutado correctamente (200)
```
