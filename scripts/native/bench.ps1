# Docker-free end-to-end run on Windows: Postgres + single-node Kafka + Prometheus (OTLP receiver) + the real apps.
# One run = fixed generator load -> relay-poller -> consumer (-> rollup-streams) -> verifier, tagged RUN_ID/SCENARIO/RELAY,
# with a measure window recorded for perf/report.py. Numbers are only comparable between runs on the same machine.
#   .\scripts\native\bench.ps1 -Name p1-baseline -Rate 1000 -Total 60000
param(
    [Parameter(Mandatory)] [string]$Name,
    [int]$Rate = 1000, [long]$Total = 60000, [string]$Scenario = 'baseline', [string]$Relay = 'poller',
    [int]$Concurrency = 3, [int]$MaxPollRecords = 500, [int]$Batch = 500,
    [double]$HotShare = 0, [string]$KeyStrategy = 'aggregate', [double]$PillRate = 0, [int]$Seed = 42,
    [int]$Partitions = 12, [switch]$Rollup, [int]$RollupBuckets = 8, [switch]$NoOrderCheck, [switch]$ReplayDlq,
    [int]$KillConsumerEverySeconds = 0, [int]$DrainMinutes = 12,
    [string]$Native = $(if ($env:PAYFLOW_NATIVE) { $env:PAYFLOW_NATIVE } else { 'C:\payflow-native' })
)
$ErrorActionPreference = 'Stop'
$root = (Resolve-Path "$PSScriptRoot\..\..").Path
$java = "$HOME\scoop\apps\temurin25-jdk\current\bin\java.exe"
$bash = 'C:\Program Files\Git\bin\bash.exe'
$prom = Get-ChildItem "$Native\prom\prometheus-*" -Directory | Select-Object -First 1
$runId = "$Name-" + (Get-Date -Format 'MMdd-HHmmss')

function Sql($q) {
    $out = & $bash "$Native/tools/sql.sh" payflow $q 2>&1
    if ($LASTEXITCODE -ne 0 -or ($out | Select-String 'Exception|ERROR:' -Quiet)) { throw "SQL failed: $q`n$($out | Select-Object -First 4)" }
    return ($out | Select-Object -First 1)
}
function Wait-Port($p, $secs) { $d = (Get-Date).AddSeconds($secs); while (-not (netstat -an | Select-String "$p.*LISTENING")) { if ((Get-Date) -gt $d) { throw "port $p not up" }; Start-Sleep 1 } }

function Ensure-Postgres {
    if (-not (netstat -an | Select-String '55432.*LISTENING')) {
        Start-Process "$Native\pg\bin\pg_ctl.exe" -ArgumentList '-D', "$Native\pgdata", '-l', "$Native\pg.log", '-o', '"-p 55432 -c wal_level=logical -c max_connections=300 -c listen_addresses=127.0.0.1"', 'start' -WindowStyle Hidden
        Wait-Port 55432 60; Start-Sleep 3
    }
}
function Ensure-Prometheus {
    if (-not (netstat -an | Select-String '127.0.0.1:19090.*LISTENING')) {
        Start-Process "$($prom.FullName)\prometheus.exe" -WorkingDirectory "$root\scripts\native" -WindowStyle Hidden `
            -ArgumentList '--config.file=prometheus-native.yml', "--storage.tsdb.path=$Native\promdata", '--web.enable-otlp-receiver',
            '--storage.tsdb.retention.time=2d', '--web.listen-address=127.0.0.1:19090' `
            -RedirectStandardOutput "$Native\prometheus.out" -RedirectStandardError "$Native\prometheus.err"
        Wait-Port 19090 60
    }
}
function Start-App([string]$jar, [string]$tag, [hashtable]$extra = @{}, [int]$heapMb = 256) {
    $env:SPRING_DATASOURCE_URL = 'jdbc:postgresql://127.0.0.1:55432/payflow'; $env:KAFKA_BOOTSTRAP = '127.0.0.1:19092'
    $env:MANAGEMENT_OTLP_METRICS_EXPORT_URL = 'http://127.0.0.1:19090/api/v1/otlp/v1/metrics'
    $env:MANAGEMENT_OPENTELEMETRY_TRACING_EXPORT_OTLP_ENDPOINT = 'http://127.0.0.1:1/v1/traces'
    $env:MANAGEMENT_OPENTELEMETRY_LOGGING_EXPORT_OTLP_ENDPOINT = 'http://127.0.0.1:1/v1/logs'
    $env:RUN_ID = $runId; $env:SCENARIO = $Scenario; $env:RELAY = $Relay; $env:SERVICE_NAME = $tag
    foreach ($k in $extra.Keys) { Set-Item "env:$k" $extra[$k] }
    $out = "$Native\$tag.out"; if (Test-Path $out) { Remove-Item $out }
    $p = Start-Process $java -ArgumentList "-Xmx${heapMb}m", '-XX:+UseSerialGC', '-jar', ('"' + "$root\$jar" + '"') -RedirectStandardOutput $out `
        -RedirectStandardError "$Native\$tag.err" -PassThru -WindowStyle Hidden
    $d = (Get-Date).AddMinutes(5)
    while (-not (Select-String -Path $out -Pattern ' Started \w+ in ' -Quiet -ErrorAction SilentlyContinue)) {
        if ((Get-Date) -gt $d -or $p.HasExited) { throw "$tag did not start (see $out)" }
        Start-Sleep 2
    }
    return $p
}
function Stop-App($p) { if ($p -and -not $p.HasExited) { Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue } }
function Consumer-Env { @{ CONCURRENCY = "$Concurrency"; MAX_POLL_RECORDS = "$MaxPollRecords"; CONSUMER_GROUP = 'payflow-consumer'; ORDER_CHECK = $(if ($NoOrderCheck) { 'false' } else { 'true' }) } }

Ensure-Postgres; Ensure-Prometheus
$env:PAYFLOW_PARTITIONS = "$Partitions"
& "$Native\tools\kafka-reset.ps1" | Out-Null
try {
"run $runId rate=$Rate total=$Total relay=$Relay partitions=$Partitions consumers=$Concurrency hot=$HotShare key=$KeyStrategy pills=$PillRate rollup=$([bool]$Rollup)"
$relayApp = $null; $consumer = $null; $rollupApp = $null; $gen = $null; $ver = $null
$relayApp = Start-App 'relay-poller\target\relay-poller-1.0.0.jar' 'relay-poller' @{ BATCH = "$Batch" } 192
# the relay start ran Flyway, so every table exists; reset them before the consumer and generator start
Sql 'truncate outbox, payments, processed_events, effect_log, aggregate_seq, merchant_balance, pills, verifier_kafka, verifier_dlq, verifier_totals' | Out-Null
$consumer = Start-App 'consumer\target\consumer-1.0.0.jar' 'payflow-consumer' (Consumer-Env)
$rollupApp = $null
if ($Rollup) {
    $rollupApp = Start-App 'rollup-streams\target\rollup-streams-1.0.0.jar' 'rollup-streams' @{ ROLLUP_REPLICATION = '1'; ROLLUP_BUCKETS = "$RollupBuckets"; ROLLUP_STATE_DIR = "$Native\rollup-state-$runId" } 384
}
$t0 = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
$gen = Start-App 'generator\target\generator-1.0.0.jar' 'generator' @{ RATE = "$Rate"; TOTAL = "$Total"; SEED = "$Seed"; HOT_SHARE = "$HotShare"; KEY_STRATEGY = $KeyStrategy; PILL_RATE = "$PillRate" }

$nextKill = if ($KillConsumerEverySeconds -gt 0) { (Get-Date).AddSeconds($KillConsumerEverySeconds) } else { $null }
while (-not $gen.HasExited) {
    Start-Sleep 2
    if ($nextKill -and (Get-Date) -gt $nextKill) {
        Stop-App $consumer
        $consumer = Start-App 'consumer\target\consumer-1.0.0.jar' 'payflow-consumer' (Consumer-Env)
        $nextKill = (Get-Date).AddSeconds($KillConsumerEverySeconds)
    }
}
$genLine = (Select-String -Path "$Native\generator.out" -Pattern 'GENERATOR_DONE' | Select-Object -Last 1).Line

# drain: every committed non-pill event processed; pills legitimately stay unprocessed (they end in the DLQ)
$deadline = (Get-Date).AddMinutes($DrainMinutes); $last = -1; $stable = 0
do {
    Start-Sleep 5
    $left = [long](Sql "select count(*) from payments p where p.run_id = '$runId' and not exists (select 1 from processed_events e where e.event_id = p.event_id) and not exists (select 1 from pills pi where pi.event_id = p.event_id)")
    if ($left -eq $last) { $stable++ } else { $stable = 0 }; $last = $left
} while ($left -gt 0 -and $stable -lt 24 -and (Get-Date) -lt $deadline)
if ($PillRate -gt 0) {
    Start-Sleep 90   # the retry path takes retry-5s then retry-1m (about 65 s) before an event reaches the DLQ
}
if ($rollupApp) { Start-Sleep 8 }   # let the last windows flush through both stages
Start-Sleep 7                       # one more OTLP push interval so the tail of the run is in Prometheus
$t1 = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()

$offs = & $java -cp "$Native\kafka\libs\*" org.apache.kafka.tools.GetOffsetShell --bootstrap-server 127.0.0.1:19092 --topic payment.events 2>$null
$perPart = @($offs | ForEach-Object { [double](($_ -split ':')[2]) })
$avg = ($perPart | Measure-Object -Average).Average; $mx = ($perPart | Measure-Object -Maximum).Maximum
$imbalance = if ($avg -gt 0) { $mx / $avg } else { 0 }
$replayLine = ''
if ($ReplayDlq) {
    $env:RUN_ID = $runId
    $rp = Start-Process $java -ArgumentList '-Xmx192m', '-cp', "$root\consumer\target\consumer-1.0.0.jar", '-Dloader.main=io.payflow.dlqreplay.DlqReplayApp',
        'org.springframework.boot.loader.launch.PropertiesLauncher' -RedirectStandardOutput "$Native\dlq-replay.out" -RedirectStandardError "$Native\dlq-replay.err" -PassThru -WindowStyle Hidden
    $rp.WaitForExit(240000) | Out-Null
    $replayLine = (Select-String -Path "$Native\dlq-replay.out" -Pattern 'DLQ_REPLAY_DONE' | Select-Object -Last 1).Line
    Start-Sleep 25   # the still-running consumer applies the replayed events (same ids, dedup keeps them exactly-once)
}
Stop-App $consumer; Stop-App $relayApp

# verifier: scans Kafka into staging tables, compares with the database, writes the row (with the measure window) to results.json
$ver = Start-App 'verifier\target\verifier-1.0.0.jar' 'verifier' @{ RESULTS_FILE = "$root\docs\results.json"; RUN_START_EPOCH = "$t0"; RUN_END_EPOCH = "$t1"; RATE = "$Rate"; BENCH_NAME = $Name; VERIFY_START_OFFSETS = ''; VERIFY_CHECK_ROLLUP = $(if ($Rollup) { 'true' } else { 'false' }) }
$ver.WaitForExit(240000) | Out-Null
$verLine = (Select-String -Path "$Native\verifier.out" -Pattern '^VERIFY ' | Select-Object -Last 1).Line
Stop-App $ver; Stop-App $rollupApp

& python "$root\scripts\native\stamp.py" "$root\docs\results.json" $runId "bench=$Name" "partitions=$Partitions" "consumers=$Concurrency" "key_strategy=$KeyStrategy" "hot_share=$HotShare" "offered_rate=$Rate" "partition_imbalance=$([math]::Round($imbalance, 3))" "pill_rate=$PillRate" "rollup=$([bool]$Rollup)" "order_check=$(-not $NoOrderCheck)"
$genLine
$replayLine
$verLine
"window: $t0 .. $t1 ($($t1 - $t0) s)  imbalance=$([math]::Round($imbalance, 3))"
} finally {
    Stop-App $gen; Stop-App $ver; Stop-App $rollupApp; Stop-App $consumer; Stop-App $relayApp
}
