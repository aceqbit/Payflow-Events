#!/bin/bash
# kill -9 the writer container every 10-30 s (seeded random), restart it, annotate Grafana.
# usage: chaos/kill-writer.sh <service> [seed] [max-seconds]   (service = dual-write-app | generator | source-app)
. "$(dirname "$0")/lib.sh"
SERVICE=${1:?service}
SEED=${2:-42}
MAX=${3:-3600}
RANDOM=$SEED
end=$((SECONDS + MAX))
kills=0
while [ $SECONDS -lt $end ]; do
  sleep $((10 + RANDOM % 21))
  # the generator exits by itself once TOTAL is committed; stop killing it then
  if [ "$(docker inspect -f '{{.State.Running}}' "$SERVICE" 2>/dev/null)" != "true" ]; then
    if [ "$(docker inspect -f '{{.State.ExitCode}}' "$SERVICE" 2>/dev/null)" = "0" ] && [ "${STOP_ON_CLEAN_EXIT:-1}" = 1 ]; then
      log "$SERVICE exited cleanly: all events written after $kills kills"
      exit 0
    fi
  else
    docker kill -s KILL "$SERVICE" >/dev/null
    kills=$((kills + 1))
    log "kill -9 $SERVICE (#$kills)"
    annotate "kill -9 $SERVICE #$kills"
  fi
  $COMPOSE --profile source --profile dualwrite --profile gen up -d "$SERVICE" >/dev/null
done
