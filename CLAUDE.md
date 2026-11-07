# PayFlow Events

Lossless, skew-resilient payment event pipeline: 0 lost, 0 phantom, 0 duplicate effects under crash
injection with a transactional outbox (dual-write is the counted baseline), flat p99 under 40% merchant
skew, healthy events flow past poison pills. Design: docs/plan.md. Outbox contract: docs/contract/outbox.sql.
Progress: docs/progress.md.

## Stack (pinned)
Java 25, Spring Boot 4.0.x, Spring for Apache Kafka 4.x, Kafka 4.1.x KRaft (3 brokers, RF=3, min.insync.replicas=2),
Debezium 3.4 (pgoutput + Outbox Event Router), PostgreSQL 18, Apicurio Registry 3, Kafka Streams exactly_once_v2,
OTel -> Collector -> Prometheus 3 / Tempo / Loki / Grafana. Tests: JUnit 5, Testcontainers, Awaitility.

## Commands
make up | down | test | it | chaos | skew | pills | rebalance | bench | report   (MATRIX=smoke for short runs)

## Delivery rules
1. Event exists only if its outbox row committed; only relays produce (dual-write-app is the measured exception).
2. Both relays emit the same record: key=aggregate_id, value=payload as-is, headers id,type,seq,occurred_at,traceparent,merchant_id.
3. Producers: enable.idempotence=true, acks=all.
4. Consumers: auto-commit off; dedup insert + effect + seq update in one DB tx; offsets after DB commit.
5. Dedup key = id header; ON CONFLICT DO NOTHING; effects only for inserted ids.
6. Partition key = aggregate_id, never merchant_id.
7. Failures -> retry topics -> DLQ; non-retryable straight to DLQ.
8. Topics only via topic-init.
9. Outbox schema is the Ledger's; V1 must equal docs/contract/outbox.sql.
10. Never edit an applied Flyway migration.

## Telemetry
All meters in common EventsMetrics; names per plan §5; no id labels (event/payment/aggregate/merchant).
Scripts set RUN_ID, SCENARIO, RELAY first. Dashboards/alerts/rules/collector/connector config live in the repo.

## Workflow
One runbook prompt = one commit: `<prompt-id>: <what changed>`. Append a line to docs/progress.md per prompt.
Numbers come only from `make report`. Never weaken a correctness gate to pass a test.
