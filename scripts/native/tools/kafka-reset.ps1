# Fresh single-node KRaft broker + payment.events (12 partitions, LogAppendTime). Never delete topics on Windows:
# Kafka on Windows shuts the broker down when it cannot delete a log directory, so a reset wipes the whole data dir.
$native = if ($env:PAYFLOW_NATIVE) { $env:PAYFLOW_NATIVE } else { 'C:\payflow-native' }
$j = "$HOME\scoop\apps\temurin25-jdk\current\bin\java.exe"
$kcp = "$native\kafka\libs\*"
Get-CimInstance Win32_Process -Filter "name='java.exe'" | Where-Object { $_.CommandLine -match 'kafka\.Kafka' } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
Start-Sleep 3
if (Test-Path "$native\kdata") { Remove-Item "$native\kdata" -Recurse -Force }
& $j -cp $kcp kafka.tools.StorageTool format -t MkU3OEVBNTcwNTJENDM2Qk -c "$native\server.properties" --ignore-formatted *> $null
Start-Process $j -ArgumentList '-Xmx512m', '-XX:+UseSerialGC', '-cp', $kcp, 'kafka.Kafka', "$native\server.properties" `
    -RedirectStandardOutput "$native\kafka.out" -RedirectStandardError "$native\kafka.err" -WindowStyle Hidden
$deadline = (Get-Date).AddSeconds(120)
while (-not (netstat -an | Select-String '19092.*LISTENING')) {
    if ((Get-Date) -gt $deadline) { throw 'kafka did not start' }
    Start-Sleep 1
}
Start-Sleep 4
$p = if ($env:PAYFLOW_PARTITIONS) { [int]$env:PAYFLOW_PARTITIONS } else { 12 }
$topics = @(
    @('payment.events', $p, @('--config', 'message.timestamp.type=LogAppendTime')),
    @('payment.events-retry-5s', $p, @()), @('payment.events-retry-1m', $p, @()),
    @('payments.dlq', 3, @()), @('merchant.rollup.stage1', 12, @()), @('merchant.totals', 6, @('--config', 'cleanup.policy=compact')))
foreach ($t in $topics) {
    & $j -cp $kcp org.apache.kafka.tools.TopicCommand --bootstrap-server 127.0.0.1:19092 --create --if-not-exists --topic $t[0] `
        --partitions $t[1] --replication-factor 1 @($t[2]) 2>&1 | Select-Object -Last 1
}
