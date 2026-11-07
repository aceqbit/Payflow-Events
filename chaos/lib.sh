#!/bin/bash
# Shared helpers for chaos and benchmark runs. Source it: . chaos/lib.sh
# Every script sets RUN_ID, SCENARIO and RELAY before it starts apps (CLAUDE.md telemetry conventions).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE=${COMPOSE:-"docker compose -f $ROOT/deploy/docker-compose.yml -f $ROOT/deploy/docker-compose.observability.yml -f $ROOT/deploy/docker-compose.connect.yml -f $ROOT/deploy/docker-compose.apps.yml"}
GRAFANA=${GRAFANA:-http://localhost:3000}
GRAFANA_AUTH=${GRAFANA_AUTH:-admin:admin}
RESULTS=${RESULTS:-$ROOT/docs/results.json}

log() { echo "[$(date +%H:%M:%S)] $*"; }

# Post a Grafana annotation tagged chaos + RUN_ID. Never fails the run.
annotate() {
  curl -s -m 5 -u "$GRAFANA_AUTH" -H 'Content-Type: application/json' -X POST "$GRAFANA/api/annotations" \
    -d "{\"tags\":[\"chaos\",\"${RUN_ID:-adhoc}\"],\"text\":\"$1\"}" >/dev/null || true
}

psql_exec() { docker exec -i postgres psql -U payflow -d payflow -v ON_ERROR_STOP=1 -tA -c "$1"; }

# Run hygiene (plan section 8): stop apps, empty tables, recreate topics, drop consumer groups.
reset_state() {
  log "reset state"
  $COMPOSE --profile source --profile dualwrite --profile poller --profile consumer --profile gen --profile verify \
    stop source-app dual-write-app relay-poller consumer generator rollup-streams 2>/dev/null || true
  psql_exec "truncate outbox, payments, processed_events, effect_log, aggregate_seq, merchant_balance, pills, verifier_kafka, verifier_dlq, verifier_totals" >/dev/null
  RECREATE=1 $COMPOSE run --rm topic-init >/dev/null
  for g in payflow-consumer dlq-replay; do
    docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --delete --group "$g" >/dev/null 2>&1 || true
  done
}

# Offsets at the start of a run, as JSON {"partition": offset}, so the verifier reads only this run.
start_offsets() {
  docker exec kafka-1 /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka-1:9092 --topic "${1:-payment.events}" \
    | awk -F: '{printf "%s\"%s\":%s", (n++?",":"{"), $2, $3} END {print "}"}'
}

committed_count() { psql_exec "select count(*) from payments where run_id = '$RUN_ID'"; }
unprocessed_count() {
  psql_exec "select count(*) from payments p where p.run_id = '$RUN_ID' and not exists (select 1 from processed_events e where e.event_id = p.event_id)"
}

# Wait (bounded) until the generator/writer finished and every committed event is processed or the deadline passes.
wait_drained() {
  local deadline=$((SECONDS + ${1:-600})) last=-1 stable=0 left
  while [ $SECONDS -lt $deadline ]; do
    left=$(unprocessed_count)
    if [ "$left" = "$last" ]; then stable=$((stable + 1)); else stable=0; fi
    last=$left
    # drained, or no progress for 60 s (dual-write runs never drain: the lost events never arrive)
    if [ "$left" = 0 ] || [ $stable -ge 12 ]; then return 0; fi
    sleep 5
  done
}

# Run the verifier for RUN_ID; extra args are KEY=VALUE env overrides.
run_verifier() {
  env RUN_ID="$RUN_ID" SCENARIO="$SCENARIO" RELAY="$RELAY" "$@" $COMPOSE --profile verify run --rm verifier
}

# Add the measure window to the run's results.json row (report.py queries Prometheus over it).
stamp_window() { # start_epoch end_epoch
  python3 - "$RESULTS" "$RUN_ID" "$1" "$2" <<'PY'
import json, sys
f, run, s, e = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
rows = json.load(open(f))
for r in rows:
    if r["run_id"] == run:
        r["run_start_epoch"], r["run_end_epoch"] = s, e
json.dump(rows, open(f, "w"), indent=2)
PY
}
