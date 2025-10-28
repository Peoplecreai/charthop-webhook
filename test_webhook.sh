#!/bin/bash
# Script para probar la sincronización de time off

set -e

# Colores para output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}=== Test ChartHop → Runn Time Off Sync ===${NC}\n"

# 1. Obtener SERVICE_URL
echo "1. Obteniendo SERVICE_URL..."
SERVICE_URL="$(gcloud run services describe charthop-webhook \
  --region northamerica-south1 --format='value(status.address.url)')"
echo -e "${GREEN}✓ SERVICE_URL: $SERVICE_URL${NC}\n"

# 2. Obtener Service Account
echo "2. Obteniendo Service Account..."
SA="$(gcloud run services describe charthop-webhook \
  --region northamerica-south1 --format='value(spec.template.spec.serviceAccountName)')"
echo -e "${GREEN}✓ Service Account: $SA${NC}\n"

# 3. Obtener ID Token
echo "3. Obteniendo ID Token..."
ID_TOKEN="$(gcloud auth print-identity-token \
  --impersonate-service-account="$SA" \
  --audiences="$SERVICE_URL")"
echo -e "${GREEN}✓ Token obtenido${NC}\n"

# 4. Test 1: Webhook de CREATE/UPDATE
echo -e "${YELLOW}4. Test 1: Simular webhook de ChartHop (CREATE/UPDATE)${NC}"
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "$SERVICE_URL/webhooks/charthop" \
  -H "Content-Type: application/json" \
  -d '{
    "type": "timeoff.create",
    "entityType": "timeoff",
    "entityId": "test-timeoff-123"
  }')

HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | head -n-1)

if [ "$HTTP_CODE" = "200" ]; then
  echo -e "${GREEN}✓ Webhook recibido correctamente (200)${NC}"
  echo "Response: $BODY"
else
  echo -e "${RED}✗ Error: HTTP $HTTP_CODE${NC}"
  echo "Response: $BODY"
fi
echo ""

# 5. Test 2: Webhook de DELETE
echo -e "${YELLOW}5. Test 2: Simular webhook de ChartHop (DELETE)${NC}"
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "$SERVICE_URL/webhooks/charthop" \
  -H "Content-Type: application/json" \
  -d '{
    "type": "timeoff.delete",
    "entityType": "timeoff",
    "entityId": "test-timeoff-123"
  }')

HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | head -n-1)

if [ "$HTTP_CODE" = "200" ]; then
  echo -e "${GREEN}✓ Webhook DELETE recibido correctamente (200)${NC}"
  echo "Response: $BODY"
else
  echo -e "${RED}✗ Error: HTTP $HTTP_CODE${NC}"
  echo "Response: $BODY"
fi
echo ""

# 6. Test 3: Worker directo (requiere payload correcto)
echo -e "${YELLOW}6. Test 3: Llamar directamente al worker${NC}"
echo "Nota: Este test fallará si el timeoff_id no existe en ChartHop"
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "$SERVICE_URL/tasks/worker" \
  -H "Authorization: Bearer $ID_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "kind": "timeoff",
    "entity_id": "67235d4eb7e4c100191a36bd"
  }')

HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | head -n-1)

if [ "$HTTP_CODE" = "200" ]; then
  echo -e "${GREEN}✓ Worker ejecutado correctamente (200)${NC}"
  echo "Response:"
  echo "$BODY" | jq '.' 2>/dev/null || echo "$BODY"
else
  echo -e "${RED}✗ Error: HTTP $HTTP_CODE${NC}"
  echo "Response: $BODY"
fi
echo ""

# 7. Verificar logs recientes
echo -e "${YELLOW}7. Logs recientes del servicio:${NC}"
gcloud logging read \
  'resource.type="cloud_run_revision"
   AND resource.labels.service_name="charthop-webhook"
   AND severity>=WARNING' \
  --project=integration-hub-468417 \
  --freshness=5m \
  --limit=5 \
  --format='table(timestamp,severity,textPayload)' 2>/dev/null || echo "No hay logs de error recientes"

echo -e "\n${GREEN}=== Tests completados ===${NC}"
