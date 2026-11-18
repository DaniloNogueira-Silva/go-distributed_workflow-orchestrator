# simulate_scenarios.ps1
# Script de simulação End-to-End para o Distributed Workflow Orchestrator & Edge Rate Limiter

$BaseUrl = "http://localhost:8080/api/v1/workflows/checkout"

Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host " CENÁRIO 1: Checkout Saga Completo (Payment -> Inventory -> Fulfillment -> Notification)" -ForegroundColor Cyan
Write-Host "==================================================================" -ForegroundColor Cyan

$bodySuccess = @{
    customer_id = "cust_001"
    amount      = 450.00
    currency    = "BRL"
    items       = @("notebook_stand", "mechanical_keyboard")
} | ConvertTo-Json

$resp1 = Invoke-RestMethod -Uri $BaseUrl -Method Post -Headers @{ "X-API-Key" = "key-scenario-1" } -ContentType "application/json" -Body $bodySuccess
Write-Host "Workflow ID: $($resp1.workflow_id) | Status Final: $($resp1.status)" -ForegroundColor Green
$resp1 | ConvertTo-Json -Depth 6

Write-Host "`n==================================================================" -ForegroundColor Yellow
Write-Host " CENÁRIO 2: Inbound Rate Limiter Bloqueando Cliente (429 Too Many Requests)" -ForegroundColor Yellow
Write-Host "==================================================================" -ForegroundColor Yellow

for ($i = 1; $i -le 7; $i++) {
    try {
        $r = Invoke-WebRequest -Uri $BaseUrl -Method Post -Headers @{ "X-API-Key" = "key-burst-client" } -ContentType "application/json" -Body $bodySuccess -UseBasicParsing
        Write-Host "Requisição #$i -> HTTP $($r.StatusCode) (Remaining Tokens: $($r.Headers['X-RateLimit-Remaining']))" -ForegroundColor Green
    } catch {
        $statusCode = [int]$_.Exception.Response.StatusCode
        Write-Host "Requisição #$i -> HTTP $statusCode BLOQUEADO pelo Rate Limiter (429 Too Many Requests)" -ForegroundColor Red
    }
}

Write-Host "`n==================================================================" -ForegroundColor Magenta
Write-Host " CENÁRIO 3: Falha no Fulfillment -> Rollback Reverso (Inventory e Payment)" -ForegroundColor Magenta
Write-Host "==================================================================" -ForegroundColor Magenta

$bodyRollback = @{
    customer_id        = "cust_003"
    amount             = 1250.00
    currency           = "BRL"
    items              = @("gpu_rtx", "psu_850w")
    simulate_fail_step = "Fulfillment"
} | ConvertTo-Json

$resp3 = Invoke-RestMethod -Uri $BaseUrl -Method Post -Headers @{ "X-API-Key" = "key-scenario-3" } -ContentType "application/json" -Body $bodyRollback
Write-Host "Workflow ID: $($resp3.workflow_id) | Status Final: $($resp3.status)" -ForegroundColor Yellow
$resp3 | ConvertTo-Json -Depth 6
