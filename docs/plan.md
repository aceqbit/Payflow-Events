# PayFlow Events v2 — design and plan (condensed from the v2 PDF)

Goals: 0 lost, 0 phantom, 0 duplicate effects, flat p99 under 40% skew, 0 events stuck behind a poison pill, one
OTel trace from Ledger HTTP request to consumer commit.

## §2 Stack
Java 25, Spring Boot 4.0.x, Kafka 4.1.x KRaft x3 (RF=3, min.isr=2), Debezium 3.4+, PostgreSQL 18, Apicurio 3
(JSON Schema, BACKWARD), zstd, Kafka Streams exactly_once_v2 (classic protocol), OTel Collector -> Prometheus 3 /
Tempo / Loki / Grafana 12.

## §3 Architecture
Record shape (both relays): key=aggregate_id; value=payload (jsonb as-is); headers id, type, seq, occurred_at,
traceparent, merchant_id; topic=<aggregate_type>.events.

| Topic | Partitions | Retention | Config |
|---|---|---|---|
| payment.events, transfer.events | 12 | 7d | LogAppendTime |
| payment.events-retry-5s, -retry-1m | 12 | 1d | non-blocking retries |
| payments.dlq | 3 | 30d | after 3 attempts |
| merchant.rollup.stage1 | 12 | 1d | salted partial sums, 1 s window |
| merchant.totals | 6 | compacted | final totals |

Relay A (poller): SELECT ... WHERE published_at IS NULL ORDER BY id LIMIT 500 FOR UPDATE SKIP LOCKED; send,
wait acks; UPDATE published_at; commit. Idle backoff 5 ms (full batch) / 50 ms (empty).
Relay B (Debezium): EventRouter on public.outbox, outbox.mode=delete in source. See connect/debezium-outbox.json.
Consumer: batch tx: INSERT processed_events ... ON CONFLICT DO NOTHING RETURNING; effects for returned ids; seq
check vs aggregate_seq; commit; then ack offsets. Failures -> retry-5s -> retry-1m -> payments.dlq.
Skew: key is aggregate_id; rollup stage1 key merchant_id#(hash(payment_id)%B), stage2 key merchant_id.

## §4 Observability
Latency split: relay = record ts (LogAppendTime) - occurred_at; consume = now after commit - record ts; e2e = sum.
SLO buckets e2e 10ms..5s; relay/consume 5ms..1s. Labels allowed: topic, partition, relay, result, kind, stage,
poison, strategy, group, scenario, run_id. Tail sampling: ERROR 100%, >200ms 100%, 2% rest.

## §5 Metrics
payflow_events_{e2e,relay,consume}_latency_seconds, _consumer_lag_seconds, _processed_total{result},
_order_violations_total{kind}, _dedup_batch_size, _dedup_insert_seconds, _retry_total{stage},
_dlq_replayed_total{result}, _consumption_gap_seconds; payflow_relay_{batch_size,poll_seconds,published_total};
payflow_generator_{sent_total,hot_share}; payflow_verifier_{lost_events,phantom_events,duplicate_effects,
duplicates_received,order_violations,dlq_count,pills_injected,last_run_timestamp_seconds};
payflow_rollup_mismatch_minor.

## §6 Dashboards / alerts: see deploy/observability.

## §7 Phase 1 exit gate
db-first loses >0; kafka-first phantom >0; outbox+poller 0/0/0/0 across crash run; baseline table; e2e trace;
telemetry overhead <5% p99; CI green.

## §8 Phase 2
CDC, polling vs CDC, chaos matrix, skew lab, spike, poison pills, rebalance benchmark, tuning T1-T6 (keep if
correctness 0 and gain >=5%).

## §10 Definition of done
make up healthy; 7 dashboards provisioned; make chaos prints counts; make skew/pills/report work; README with
charts; DLQ replay documented; trace screenshot; CI green.
