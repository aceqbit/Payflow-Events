# One-time setup for running the whole pipeline on Windows without Docker. Installs into $env:PAYFLOW_NATIVE
# (default C:\payflow-native): Postgres 18 (zonky binaries from Maven Central), Kafka 4.x KRaft (single node),
# Prometheus 3 (OTLP receiver). JDK 25 and Maven come from scoop. No admin rights needed.
#   .\scripts\native\setup.ps1
param([string]$Native = $(if ($env:PAYFLOW_NATIVE) { $env:PAYFLOW_NATIVE } else { 'C:\payflow-native' }),
      [string]$PgVersion = '18.6.0', [string]$PromVersion = '3.1.0')
$ErrorActionPreference = 'Stop'
$here = $PSScriptRoot
New-Item -ItemType Directory -Force "$Native\tools", "$Native\pg", "$Native\prom" | Out-Null

if (-not (Get-Command scoop -ErrorAction SilentlyContinue)) { throw 'install scoop first: https://scoop.sh' }
scoop bucket add java 2>$null | Out-Null
foreach ($pkg in 'maven', 'temurin25-jdk', 'kafka') { if (-not (scoop list 2>$null | Select-String "^\s*$pkg ")) { scoop install $pkg } }

# Postgres (logical replication capable build)
if (-not (Test-Path "$Native\pg\bin\postgres.exe")) {
    $jar = "$Native\pg.jar"
    Invoke-WebRequest "https://repo1.maven.org/maven2/io/zonky/test/postgres/embedded-postgres-binaries-windows-amd64/$PgVersion/embedded-postgres-binaries-windows-amd64-$PgVersion.jar" -OutFile $jar
    Expand-Archive $jar "$Native\pg" -Force
    tar -xJf "$Native\pg\postgres-windows-x86_64.txz" -C "$Native\pg"
}
if (-not (Test-Path "$Native\pgdata\PG_VERSION")) {
    Set-Content "$Native\pw" 'payflow' -NoNewline
    & "$Native\pg\bin\initdb.exe" -D "$Native\pgdata" -U payflow --pwfile="$Native\pw" -A scram-sha-256 -E UTF8
}

# Kafka: copy to a short path (the .bat launchers break on long paths; we start the JVM directly)
if (-not (Test-Path "$Native\kafka\libs")) { Copy-Item "$HOME\scoop\apps\kafka\current" "$Native\kafka" -Recurse }
Copy-Item "$here\server.properties" "$Native\server.properties" -Force

# Prometheus
if (-not (Get-ChildItem "$Native\prom\prometheus-*" -Directory -ErrorAction SilentlyContinue)) {
    $z = "$Native\prom.zip"
    Invoke-WebRequest "https://github.com/prometheus/prometheus/releases/download/v$PromVersion/prometheus-$PromVersion.windows-amd64.zip" -OutFile $z
    Expand-Archive $z "$Native\prom" -Force
}

# SQL helper (JDBC, because the Postgres bundle has no psql)
Copy-Item "$here\tools\*" "$Native\tools" -Force
$pgJar = Get-ChildItem "$HOME\.m2\repository\org\postgresql\postgresql" -Recurse -Filter 'postgresql-*.jar' -ErrorAction SilentlyContinue | Select-Object -First 1
if ($pgJar) { Copy-Item $pgJar.FullName "$Native\tools\pg.jar" -Force } else { Write-Warning 'build once with mvn so the postgres driver is in ~/.m2, then rerun setup' }
Write-Host "native stack ready in $Native"
