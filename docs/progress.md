# Progress

| id | change | check | RUN_IDs |
|----|--------|-------|---------|
| setup | CLAUDE.md, plan, outbox contract | n/a | - |
| P1-01 | maven multi-module, compose (3 KRaft brokers, pg18, registry, kafka-ui, toxiproxy, topic-init), Makefile | not run: no JDK/Docker on authoring machine | - |
| P1-02 | otel collector, prometheus3, tempo, loki, grafana datasources, JMX rules, compose overlay | not run | - |
| P1-03 | common: Headers, RecordShape (compact payload), EventsMetrics, Salting, V1-V5 migrations, JSON schemas, schema-compat script (verified: rejects broken schema), telemetry yml | schema-compat verified locally; Java/IT not run (no JDK) | - |
| P1-04 | common: UuidV7, PaymentPayload, PacedLoad, LoadLifecycle, Traceparent (+tests). source-app: outbox+payments in one stmt/tx, RATE, outbox.mode retain/delete, IT. dual-write-app: db-first / kafka-first (0.1% forced rollback), RecordShape records, IT. compose apps overlay, make images/source/dualwrite | not run: no JDK/Docker on authoring machine; 5k/s x 60s Grafana check pending | - |
