# The measured suite behind docs/results.md (Docker-free, single node). Each run resets Kafka and the tables.
#   .\scripts\native\suite.ps1            # about 45 minutes on a laptop
param([string]$Native = $(if ($env:PAYFLOW_NATIVE) { $env:PAYFLOW_NATIVE } else { 'C:\payflow-native' }),
      [string[]]$Only = @())
$bench = Join-Path $PSScriptRoot 'bench.ps1'
$log = "$Native\suite.log"
function Run($name, [hashtable]$opts) {
    if ($Only.Count -gt 0 -and $Only -notcontains $name) { return }
    "=== $name $(Get-Date -Format HH:mm:ss)" | Tee-Object -FilePath $log -Append
    try { & $bench -Name $name @opts 2>&1 | Tee-Object -FilePath $log -Append | Select-String 'GENERATOR_DONE|DLQ_REPLAY_DONE|^VERIFY|^window' }
    catch { "FAILED $name : $_" | Tee-Object -FilePath $log -Append }
}

# Phase 1 baseline: uniform load, entity key, polling relay, one consumer thread per 4 partitions
Run 'p1-baseline'    @{ Rate = 1000; Total = 60000 }

# Skew lab: one merchant carries 40% of events. Order check is off because merchant/salted keys share one key across many aggregates.
$skew = @{ Rate = 1500; Total = 90000; HotShare = 0.4; Scenario = 'skew'; NoOrderCheck = $true }
Run 'skew-merchant'  ($skew + @{ KeyStrategy = 'merchant' })
Run 'skew-aggregate' ($skew + @{ KeyStrategy = 'aggregate' })
Run 'skew-salted'    ($skew + @{ KeyStrategy = 'salted'; Rollup = $true })

# Poison pills: same load with and without 0.1% pills; the pills run also replays the DLQ
Run 'pills-off'      @{ Rate = 1000; Total = 60000; Scenario = 'pills' }
Run 'pills-on'       @{ Rate = 1000; Total = 60000; Scenario = 'pills'; PillRate = 0.001; ReplayDlq = $true }
"SUITE DONE $(Get-Date -Format HH:mm:ss)" | Tee-Object -FilePath $log -Append
