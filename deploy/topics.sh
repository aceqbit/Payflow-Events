#!/bin/bash
# Only place topics are created (plan §3). Idempotent; set RECREATE=1 to drop first.
set -euo pipefail
BS=${BOOTSTRAP:-kafka-1:9092,kafka-2:9092,kafka-3:9092}
K=/opt/kafka/bin/kafka-topics.sh
EVENT_PARTITIONS=${EVENT_PARTITIONS:-12}
COMMON="--replication-factor 3 --config min.insync.replicas=2"
DAY=86400000

mk() { # name partitions extra-config...
  local name=$1 parts=$2; shift 2
  [ "${RECREATE:-0}" = 1 ] && $K --bootstrap-server $BS --delete --topic "$name" --if-exists || true
  local cfg=(); for c in "$@"; do cfg+=(--config "$c"); done
  $K --bootstrap-server $BS --create --if-not-exists --topic "$name" --partitions "$parts" $COMMON "${cfg[@]}"
}

for t in payment.events transfer.events; do
  mk $t $EVENT_PARTITIONS message.timestamp.type=LogAppendTime retention.ms=$((7*DAY)) compression.type=zstd
done
mk payment.events-retry-5s $EVENT_PARTITIONS retention.ms=$DAY
mk payment.events-retry-1m $EVENT_PARTITIONS retention.ms=$DAY
mk payments.dlq 3 retention.ms=$((30*DAY))
mk merchant.rollup.stage1 12 retention.ms=$DAY
mk merchant.totals 6 cleanup.policy=compact
$K --bootstrap-server $BS --describe | grep -E "^Topic:" 
