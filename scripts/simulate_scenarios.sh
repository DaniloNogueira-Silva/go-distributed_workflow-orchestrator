#!/usr/bin/env bash
set -e

BASE_URL="http://localhost:8080/api/v1/workflows/checkout"

echo "=================================================================="
echo " SCENARIO 1: Successful Checkout Saga (Payment -> Inventory -> Fulfillment -> Notification)"
echo "=================================================================="
curl -i -s -X POST "$BASE_URL" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: bash-scenario-1" \
  -d '{"customer_id":"cust_001","amount":450.00,"currency":"BRL","items":["notebook_stand","mechanical_keyboard"]}'
echo -e "\n"

echo "=================================================================="
echo " SCENARIO 2: Inbound Rate Limiter Blocking Client (429 Too Many Requests)"
echo "=================================================================="
for i in {1..7}; do
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$BASE_URL" \
    -H "Content-Type: application/json" \
    -H "X-API-Key: bash-burst-client" \
    -d '{"customer_id":"cust_burst","amount":50.00,"currency":"USD","items":["item_1"]}')
  echo "Request #$i -> HTTP $STATUS"
done
echo ""

echo "=================================================================="
echo " SCENARIO 3: Fulfillment Fails -> Reverse Compensations (Inventory -> Payment)"
echo "=================================================================="
curl -i -s -X POST "$BASE_URL" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: bash-scenario-3" \
  -d '{"customer_id":"cust_003","amount":1250.00,"currency":"BRL","items":["gpu_rtx","psu_850w"],"simulate_fail_step":"Fulfillment"}'
echo -e "\n"
