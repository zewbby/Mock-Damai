param([switch]$App)
$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
Set-Location -LiteralPath $repoRoot
$envPath = Join-Path $repoRoot 'deploy/windows/.env.local'
foreach ($command in @('docker', 'java', 'mvn')) { Get-Command $command -ErrorAction Stop | Out-Null }
function New-LocalSecret {
    $bytes = New-Object byte[] 32
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try { $rng.GetBytes($bytes) } finally { $rng.Dispose() }
    return ([BitConverter]::ToString($bytes)).Replace('-', '').ToLowerInvariant()
}
if (-not (Test-Path -LiteralPath $envPath)) {
    @(
        "SMART_TICKET_DB_PASSWORD=$(New-LocalSecret)"
        "SMART_TICKET_JWT_SECRET=$(New-LocalSecret)"
        "SMART_TICKET_AUDIENCE_IDENTITY_ENCRYPTION_SECRET=$(New-LocalSecret)"
        "SMART_TICKET_PAYMENT_MOCK_CALLBACK_SECRET=$(New-LocalSecret)"
    ) | Set-Content -LiteralPath $envPath -Encoding ascii
}
foreach ($line in Get-Content -LiteralPath $envPath) {
    if ($line -match '^([A-Z_]+)=([a-f0-9]{64})$') {
        [Environment]::SetEnvironmentVariable($Matches[1], $Matches[2], 'Process')
    } else { throw 'Invalid .env.local; expected KEY=64 hex characters.' }
}
$env:SPRING_PROFILES_ACTIVE = 'local,flash-sale'
$env:SMART_TICKET_DB_URL = 'jdbc:mysql://127.0.0.1:13306/mock_damai?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=GMT%2B8'
$env:SMART_TICKET_DB_USERNAME = 'root'
$env:SMART_TICKET_REDIS_HOST = '127.0.0.1'
$env:SMART_TICKET_REDIS_PORT = '16379'
$env:SMART_TICKET_REDIS_DATABASE = '0'
$env:SMART_TICKET_REDIS_PASSWORD = ''
$env:SMART_TICKET_ROCKETMQ_NAME_SERVER = '127.0.0.1:19876'
$env:SMART_TICKET_LOCAL_MESSAGE_SENDER_ENABLED = 'false'
$env:SMART_TICKET_DOMAIN_EVENT_ENABLED = 'false'
$localConfig = Join-Path $repoRoot 'src/main/resources/application-local.yml'
if (-not (Test-Path -LiteralPath $localConfig)) {
    Copy-Item -LiteralPath 'src/main/resources/application-local.example.yml' -Destination $localConfig
}
$composeArgs = @('compose', '--env-file', $envPath, '-f', 'deploy/windows/compose.yml')
& docker @composeArgs config --quiet
if ($LASTEXITCODE -ne 0) { throw 'Compose configuration failed.' }
& docker @composeArgs up -d
if ($LASTEXITCODE -ne 0) { throw 'Infrastructure startup failed.' }
$deadline = (Get-Date).AddMinutes(4)
$ready = $false
do {
    $mysqlId = & docker @composeArgs ps -q mysql
    $redisId = & docker @composeArgs ps -q redis
    $mysqlHealth = if ($mysqlId) { & docker inspect --format '{{.State.Health.Status}}' $mysqlId }
    $redisHealth = if ($redisId) { & docker inspect --format '{{.State.Health.Status}}' $redisId }
    # Broker startup errors are expected during polling in Windows PowerShell 5.1.
    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $cluster = & docker @composeArgs exec -T broker sh mqadmin clusterList -n namesrv:9876 2>$null
        $clusterExitCode = $LASTEXITCODE
    } finally { $ErrorActionPreference = $previousPreference }
    if ($mysqlHealth -eq 'healthy' -and $redisHealth -eq 'healthy' -and $clusterExitCode -eq 0 -and ($cluster -match ':20911\s+.*\btrue\b')) {
        $ready = $true
        break
    }
    Start-Sleep -Seconds 5
} while ((Get-Date) -lt $deadline)
if (-not $ready) { throw 'Readiness timeout. Inspect docker compose logs; do not delete volumes to hide failure.' }
Write-Host 'MySQL schema ready, Redis ready, RocketMQ broker activated.'
if ($App) {
    & mvn spring-boot:run
    if ($LASTEXITCODE -ne 0) { throw 'Application exited with failure.' }
} else {
    Write-Host 'Run: powershell -ExecutionPolicy Bypass -File scripts/windows/start.ps1 -App'
}
