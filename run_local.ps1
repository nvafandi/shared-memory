<# 
Run Shared-Memory MCP Server locally (Windows + Docker Desktop)
No Nginx, no SSL - direct access to localhost:8000
#>

# Change to the project root (edit this path)
Set-Location "C:\path\to\shared-memory"

# 1. Check .env
if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Created .env from template. Edit it first!" -ForegroundColor Yellow
    notepad .env
    exit 1
}

# 2. Generate keys if still placeholder
$envContent = Get-Content .env -Raw
if ($envContent -match 'REPLACE_WITH') {
    $mcpKey = [System.Convert]::ToBase64String((1..32 | ForEach-Object { Get-Random -Maximum 256 }))
    $adminKey = [System.Convert]::ToBase64String((1..32 | ForEach-Object { Get-Random -Maximum 256 }))
    $envContent = $envContent -replace 'MCP_API_KEY=.*', "MCP_API_KEY=$mcpKey"
    $envContent = $envContent -replace 'ADMIN_API_KEY=.*', "ADMIN_API_KEY=$adminKey"
    $envContent | Set-Content .env
    Write-Host "Generated new keys in .env" -ForegroundColor Green
}

# 3. Start Docker Compose
Write-Host "Starting Docker Compose..." -ForegroundColor Cyan
docker compose up -d --build

# 4. Wait for health
Write-Host "Waiting for MCP server..." -ForegroundColor Cyan
$maxTries = 30
for ($i=1; $i -le $maxTries; $i++) {
    try {
        $resp = Invoke-WebRequest -Uri "http://localhost:8000/health" -Method GET -UseBasicParsing -ErrorAction Stop
        if ($resp.StatusCode -eq 200) {
            Write-Host "MCP Server ready at http://localhost:8000" -ForegroundColor Green
            break
        }
    } catch {}
    Start-Sleep 2
}

# 5. Show info
Write-Host ""
Write-Host "=== MCP Server Running ===" -ForegroundColor Cyan
Write-Host "SSE Endpoint: http://localhost:8000/sse"
Write-Host "Messages:     http://localhost:8000/messages/"
Write-Host "Health:       http://localhost:8000/health"
Write-Host ""
Write-Host "=== Client Config (.env) ===" -ForegroundColor Cyan
$envContent = Get-Content .env -Raw
if ($envContent -match 'MCP_API_KEY=(.+)') { $mcpKey = $matches[1] } else { $mcpKey = "NOT_SET" }
if ($envContent -match 'ADMIN_API_KEY=(.+)') { $adminKey = $matches[1] } else { $adminKey = "NOT_SET" }
Write-Host "MCP_SERVER_URL=http://localhost:8000"
Write-Host "MCP_API_KEY=$mcpKey"
Write-Host "ADMIN_API_KEY=$adminKey"
Write-Host ""
Write-Host "=== Test with MCP CLI ===" -ForegroundColor Cyan
Write-Host '$env:MCP_SERVER_URL="http://localhost:8000"'
Write-Host '$env:MCP_API_KEY="' + $mcpKey + '"'
Write-Host 'mcp list-tools --server sse:$env:MCP_SERVER_URL/sse'
Write-Host ""
Write-Host "=== Test with Python Client ===" -ForegroundColor Cyan
Write-Host "cd <client-folder>"
Write-Host "copy .env.client.example .env"
Write-Host "# edit .env with above values"
Write-Host "python client_example.py"