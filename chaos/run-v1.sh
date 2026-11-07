#!/bin/bash
# make chaos-v1: three runs of TOTAL events (default 1M), the writer killed every 10-30 s, each ending with the verifier.
#   1. dual-write db-first      2. dual-write kafka-first      3. generator (outbox) + relay-poller + consumer
# Exit status: non-zero if the outbox run is not clean. The dual-write rows are expected to show losses/phantoms.
. "$(dirname "$0")/lib.sh"
TOTAL=${TOTAL:-1000000}
RATE=${RATE:-5000}
SEED=${SEED:-42}
STAMP=$(date +%Y%m%d-%H%M%S)
export TOTAL RATE SEED
status=0

# one_run <name> <scenario> <relay> <writer-service> <writer-profile> [ENV=VALUE ...]
one_run() {
  local name=$1 scenario=$2 relay=$3 writer=$4 profile=$5
  shift 5
  export SCENARIO=$scenario RELAY=$relay RUN_ID="v1-$name-$STAMP"
  reset_state
  log "run $RUN_ID: $TOTAL events at $RATE/s, writer=$writer"
  local offsets; offsets=$(start_offsets)
  local t0; t0=$(date +%s)
  $COMPOSE --profile consumer up -d consumer >/dev/null
  if [ "$relay" = poller ]; then $COMPOSE --profile poller up -d relay-poller >/dev/null; fi
  annotate "start $RUN_ID"
  env "$@" $COMPOSE --profile "$profile" up -d "$writer" >/dev/null
  "$ROOT/chaos/kill-writer.sh" "$writer" "$SEED" 7200 &
  local killer=$!
  # the writer exits 0 once TOTAL committed events exist (it counts committed rows, so a kill never changes the total)
  docker wait "$writer" >/dev/null || true
  kill "$killer" 2>/dev/null || true
  wait_drained 900
  local t1; t1=$(date +%s)
  annotate "end $RUN_ID"
  local expect=false
  [ "$relay" = poller ] && expect=true
  if ! run_verifier VERIFY_START_OFFSETS="$offsets" VERIFY_EXPECT_CLEAN=$expect; then
    [ "$relay" = poller ] && status=1
  fi
  stamp_window "$t0" "$t1"
  $COMPOSE --profile consumer --profile poller stop consumer relay-poller >/dev/null 2>&1 || true
}

one_run dbfirst    chaos-writer dual-write dual-write-app dualwrite DUALWRITE_MODE=db-first
one_run kafkafirst chaos-writer dual-write dual-write-app dualwrite DUALWRITE_MODE=kafka-first
one_run poller     chaos-writer poller     generator      gen

log "results rows:"
python3 - "$RESULTS" "$STAMP" <<'PY'
import json, sys
rows = [r for r in json.load(open(sys.argv[1])) if r["run_id"].endswith(sys.argv[2])]
for r in rows:
    print(f'{r["run_id"]:<34} committed={r["committed"]:<8} lost={r["lost"]:<6} phantom={r["phantom"]:<6} '
          f'dup_effects={r["duplicate_effects"]:<4} order={r["order_violations"]:<4} clean={r["outbox_clean"]}')
PY
exit $status
