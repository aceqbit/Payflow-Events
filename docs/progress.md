# Progress

| id | change | check | RUN_IDs |
|----|--------|-------|---------|
| setup | CLAUDE.md, plan, outbox contract | n/a | - |
| P1-01 | maven multi-module, compose (3 KRaft brokers, pg18, registry, kafka-ui, toxiproxy, topic-init), Makefile | not run: no JDK/Docker on authoring machine | - |
| P1-02 | otel collector, prometheus3, tempo, loki, grafana datasources, JMX rules, compose overlay | not run | - |
| P1-03 | common: Headers, RecordShape (compact payload), EventsMetrics, Salting, V1-V5 migrations, JSON schemas, schema-compat script (verified: rejects broken schema), telemetry yml | schema-compat verified locally; Java/IT not run (no JDK) | - |
| P1-04 | common: UuidV7, PaymentPayload, PacedLoad, LoadLifecycle, Traceparent (+tests). source-app: outbox+payments in one stmt/tx, RATE, outbox.mode retain/delete. dual-write-app: db-first / kafka-first (0.1% forced rollback), RecordShape records. compose apps overlay, make images/source/dualwrite | JDK25+Maven: build OK, 10 unit tests pass. Native smoke (no Docker: no admin/WSL; Postgres 18.6 + single-node Kafka 4.4 on Windows, not RF=3): source-app 4.3k ev/s, 237,665 outbox = 237,665 payments, 0 orphans; db-first kill -9: 3,474 committed events never reached Kafka; kafka-first kill -9: 41 phantoms of 40,442 committed. Smoke only, not make report numbers. ITs (SourceWriterIT, DualWriterIT) and 5k/s x 60s Grafana check NOT run | p104-source-retain, p104-dw-db-first, p104-dw-kafka-first |
