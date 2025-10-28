# 🚀 Instrucciones de Deploy y Testing

## ⚠️ **Problema Actual: 503 Service Unavailable**

El error **503** que estás viendo indica que el servicio en Cloud Run **no se ha actualizado** con el nuevo código. Necesitas hacer un redeploy.

---

## 📋 **Pasos para Resolver**

### 1. **Verificar el estado actual del servicio**

```bash
gcloud run services describe charthop-webhook \
  --region northamerica-south1 \
  --format='value(status.conditions[0].message)'
```

### 2. **Ver logs de error del servicio**

```bash
gcloud logging read \
  'resource.type="cloud_run_revision"
   AND resource.labels.service_name="charthop-webhook"
   AND severity>=ERROR' \
  --project=integration-hub-468417 \
  --freshness=30m \
  --limit=20 \
  --format=json | jq -r '.[] | "\(.timestamp)  \(.severity)  \(.textPayload // .jsonPayload.message)"'
```

### 3. **Redeploy del servicio**

Hay dos opciones:

#### **Opción A: Redeploy automático (recomendado)**

Si tienes configurado CI/CD (GitHub Actions, Cloud Build, etc.):

```bash
# 1. Hacer push de tus cambios
git push origin main

# 2. Esperar a que el deploy automático termine
gcloud run services describe charthop-webhook \
  --region northamerica-south1 \
  --format='value(status.latestReadyRevisionName)'
```

#### **Opción B: Redeploy manual**

Si necesitas deployar manualmente:

```bash
# 1. Build de la imagen Docker
gcloud builds submit --tag gcr.io/integration-hub-468417/charthop-webhook

# 2. Deploy a Cloud Run
gcloud run deploy charthop-webhook \
  --image gcr.io/integration-hub-468417/charthop-webhook \
  --region northamerica-south1 \
  --platform managed
```

### 4. **Verificar que el nuevo deploy está activo**

```bash
# Ver la última revision
gcloud run revisions list \
  --service charthop-webhook \
  --region northamerica-south1 \
  --limit=1 \
  --format='table(metadata.name,status.conditions[0].status,metadata.creationTimestamp)'
```

### 5. **Probar el servicio**

Una vez que el deploy esté completo:

```bash
# Dar permisos de ejecución al script de test
chmod +x test_webhook.sh

# Ejecutar tests
./test_webhook.sh
```

---

## 🧪 **Tests Manuales**

### Test 1: Health Check

```bash
SERVICE_URL="$(gcloud run services describe charthop-webhook \
  --region northamerica-south1 --format='value(status.address.url)')"

curl -i "$SERVICE_URL/health"
# Debería retornar: 200 OK
```

### Test 2: Webhook de CREATE/UPDATE

```bash
curl -X POST "$SERVICE_URL/webhooks/charthop" \
  -H "Content-Type: application/json" \
  -d '{
    "type": "timeoff.create",
    "entityType": "timeoff",
    "entityId": "test-123"
  }'
# Debería retornar: 200 (vacío)
```

### Test 3: Webhook de DELETE

```bash
curl -X POST "$SERVICE_URL/webhooks/charthop" \
  -H "Content-Type: application/json" \
  -d '{
    "type": "timeoff.delete",
    "entityType": "timeoff",
    "entityId": "test-123"
  }'
# Debería retornar: 200 (vacío)
```

### Test 4: Worker directo

```bash
SA="$(gcloud run services describe charthop-webhook \
  --region northamerica-south1 \
  --format='value(spec.template.spec.serviceAccountName)')"

ID_TOKEN="$(gcloud auth print-identity-token \
  --impersonate-service-account="$SA" \
  --audiences="$SERVICE_URL")"

curl -X POST "$SERVICE_URL/tasks/worker" \
  -H "Authorization: Bearer $ID_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "kind": "timeoff",
    "entity_id": "67235d4eb7e4c100191a36bd"
  }'
# Nota: Debes usar un entity_id válido de ChartHop
```

---

## ❌ **Errores Comunes**

### Error: "kind" incorrecto

**❌ Incorrecto:**
```json
{
  "kind": "time_off.created",  // MAL
  "entity_id": "123"
}
```

**✅ Correcto:**
```json
{
  "kind": "timeoff",  // BIEN
  "entity_id": "123"
}
```

### Error: 503 Service Unavailable

**Causa**: El servicio no se ha redeployado con el nuevo código

**Solución**: Ver paso 3 arriba (Redeploy del servicio)

### Error: 405 Method Not Allowed

**Causa**: Estás usando GET en lugar de POST

**Solución**: Agregar `-X POST` al curl

---

## 📊 **Verificar que las mejoras funcionan**

### 1. Verificar Rate Limiting

```bash
# Hacer múltiples requests rápidos
for i in {1..5}; do
  curl -s -o /dev/null -w "%{http_code}\n" \
    -X POST "$SERVICE_URL/webhooks/charthop" \
    -H "Content-Type: application/json" \
    -d '{"type":"timeoff.create","entityType":"timeoff","entityId":"test-'$i'"}'
done
# Todos deberían retornar 200
```

### 2. Verificar Mapeo de IDs

Después de sincronizar un time off, verifica en GCS:

```bash
# Listar archivos de estado
gsutil ls gs://YOUR_BUCKET/state/

# Ver contenido del mapeo
gsutil cat gs://YOUR_BUCKET/state/timeoff_mapping.json | jq '.'
```

### 3. Verificar Métricas

```bash
# Ver métricas
gsutil cat gs://YOUR_BUCKET/state/sync_metrics.json | jq '.counters'

# Debería mostrar algo como:
# {
#   "timeoff_synced": 10,
#   "timeoff_updated": 2,
#   "timeoff_deleted": 1,
#   "timeoff_skipped": 3,
#   "timeoff_errors": 0
# }
```

### 4. Verificar Caché

```bash
# Hacer dos requests idénticos y comparar tiempos
time curl -s -X POST "$SERVICE_URL/webhooks/charthop" \
  -H "Content-Type: application/json" \
  -d '{"type":"timeoff.create","entityType":"timeoff","entityId":"test-cache"}'

# El segundo debería ser más rápido (caché activo)
time curl -s -X POST "$SERVICE_URL/webhooks/charthop" \
  -H "Content-Type: application/json" \
  -d '{"type":"timeoff.create","entityType":"timeoff","entityId":"test-cache"}'
```

---

## 🆘 **Si sigues teniendo problemas**

### 1. Ver logs en tiempo real

```bash
gcloud logging tail "resource.type=cloud_run_revision \
  AND resource.labels.service_name=charthop-webhook" \
  --project=integration-hub-468417
```

### 2. Verificar variables de entorno

```bash
gcloud run services describe charthop-webhook \
  --region northamerica-south1 \
  --format='value(spec.template.spec.containers[0].env)'
```

### 3. Verificar permisos del Service Account

```bash
SA="$(gcloud run services describe charthop-webhook \
  --region northamerica-south1 \
  --format='value(spec.template.spec.serviceAccountName)')"

gcloud projects get-iam-policy integration-hub-468417 \
  --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount:$SA"
```

### 4. Rollback si es necesario

```bash
# Ver revisiones anteriores
gcloud run revisions list \
  --service charthop-webhook \
  --region northamerica-south1

# Rollback a una revisión anterior
gcloud run services update-traffic charthop-webhook \
  --region northamerica-south1 \
  --to-revisions REVISION_NAME=100
```

---

## ✅ **Checklist de Deploy**

- [ ] Código pusheado al repositorio
- [ ] Build exitoso (sin errores)
- [ ] Deploy completado
- [ ] Health check responde 200
- [ ] Webhook de create/update funciona
- [ ] Webhook de delete funciona
- [ ] Logs no muestran errores
- [ ] Métricas se están guardando
- [ ] Mapeo de IDs funciona
- [ ] Caché funcionando

---

## 📞 **Contacto**

Si necesitas ayuda adicional, incluye en tu reporte:
1. Output completo del comando `./test_webhook.sh`
2. Logs de error recientes
3. Última revisión activa
4. Variables de entorno del servicio
