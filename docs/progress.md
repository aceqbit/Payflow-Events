# Progress

| id | change | check | RUN_IDs |
|----|--------|-------|---------|
| setup | CLAUDE.md, plan, outbox contract | n/a | - |
| P1-01 | maven multi-module, compose (3 KRaft brokers, pg18, registry, kafka-ui, toxiproxy, topic-init), Makefile | not run: no JDK/Docker on authoring machine | - |
| P1-02 | otel collector, prometheus3, tempo, loki, grafana datasources, JMX rules, compose overlay | not run | - |
| P1-03 | common: Headers, RecordShape (compact payload), EventsMetrics, Salting, V1-V5 migrations, JSON schemas, schema-compat script (verified: rejects broken schema), telemetry yml | schema-compat verified locally; Java/IT not run (no JDK) | - |
