#!/usr/bin/env python3
"""Generates the seven provisioned Grafana dashboards (plan section 6) into deploy/observability/grafana/dashboards.
Run `python perf/gen_dashboards.py`; the JSON is committed. Every dashboard has run_id / scenario / relay variables and
shows generator and chaos events as annotations. Metric names are exactly plan section 5."""
import json
from pathlib import Path

OUT = Path(__file__).resolve().parent.parent / "deploy" / "observability" / "grafana" / "dashboards"
DS = {"type": "prometheus", "uid": "prometheus"}
RUN = 'run_id=~"$run_id"'


def var(name, label_query, allv=True):
    return {"name": name, "label": name, "type": "query", "datasource": DS, "includeAll": allv, "multi": True,
            "allValue": ".*", "refresh": 2, "current": {"selected": True, "text": "All", "value": "$__all"},
            "query": {"query": label_query, "refId": f"v_{name}"}}


class Dash:
    def __init__(self, uid, title):
        self.uid, self.title, self.panels, self.y = uid, title, [], 0
        self.next_id = 1

    def panel(self, title, exprs, kind="timeseries", unit=None, w=12, h=8, x=None, legend=None, extra=None):
        if x is None:
            x = 0 if (sum(p["gridPos"]["w"] for p in self.panels if p["gridPos"]["y"] == self.y) % 24 == 0) else \
                sum(p["gridPos"]["w"] for p in self.panels if p["gridPos"]["y"] == self.y)
        if x + w > 24:
            self.y += h
            x = 0
        targets = [{"refId": chr(65 + i), "datasource": DS, "expr": e, "legendFormat": (legend[i] if legend else "{{run_id}}")}
                   for i, e in enumerate(exprs)]
        p = {"id": self.next_id, "title": title, "type": kind, "datasource": DS, "targets": targets,
             "gridPos": {"h": h, "w": w, "x": x, "y": self.y},
             "fieldConfig": {"defaults": ({"unit": unit} if unit else {}), "overrides": []}, "options": {}}
        if extra:
            p.update(extra)
        self.next_id += 1
        self.panels.append(p)
        if x + w >= 24:
            self.y += h
        return self

    def row(self, y_advance=0):
        self.y += y_advance
        return self

    def build(self):
        return {
            "uid": self.uid, "title": self.title, "schemaVersion": 39, "version": 1, "editable": True, "refresh": "10s",
            "time": {"from": "now-30m", "to": "now"}, "tags": ["payflow"], "timezone": "browser",
            "templating": {"list": [var("run_id", "label_values(payflow_events_processed_total, run_id)"),
                                    var("scenario", "label_values(payflow_verifier_lost_events, scenario)"),
                                    var("relay", "label_values(payflow_events_e2e_latency_seconds_count, relay)")]},
            "annotations": {"list": [
                {"name": "Generator and chaos events", "datasource": DS, "enable": True, "iconColor": "orange",
                 "expr": "changes(payflow_generator_sent_total[1m]) > 0 or changes(payflow_verifier_last_run_timestamp_seconds[1m]) > 0"},
                {"name": "Chaos annotations", "datasource": {"type": "grafana", "uid": "-- Grafana --"}, "enable": True,
                 "iconColor": "red", "target": {"type": "tags", "tags": ["chaos"], "limit": 100, "matchAny": True}}]},
            "panels": self.panels}

    def write(self):
        OUT.mkdir(parents=True, exist_ok=True)
        (OUT / f"{self.uid}.json").write_text(json.dumps(self.build(), indent=2), encoding="utf-8")


def pipeline():
    d = Dash("pipeline-overview", "Pipeline Overview")
    d.panel("Events/s in vs out", ['sum by (run_id) (rate(payflow_generator_sent_total{%s}[1m]))' % RUN,
                                   'events:throughput:1m{%s}' % RUN], legend=["in {{run_id}}", "out {{relay}} {{run_id}}"], unit="ops")
    d.panel("e2e p99 by relay", ['events:e2e_p99:1m{%s}' % RUN], legend=["{{relay}} {{run_id}}"], unit="s")
    d.panel("Max lag (s)", ['events:max_lag_seconds{%s}' % RUN], kind="stat", unit="s", w=6, h=5)
    d.panel("DLQ received (5m)", ['sum(increase(payflow_events_retry_total{stage="dlq"}[5m]))'], kind="stat", w=6, h=5,
            legend=["dlq"])
    d.panel("Verifier status (lost + phantom + dup effects)",
            ['max(payflow_verifier_lost_events{relay!="dual-write"}) + max(payflow_verifier_phantom_events{relay!="dual-write"}) + max(payflow_verifier_duplicate_effects)'],
            kind="stat", w=12, h=5, legend=["outbox failures"])
    d.panel("Processed by result", ['sum by (result) (rate(payflow_events_processed_total{%s}[1m]))' % RUN],
            legend=["{{result}}"], unit="ops")
    d.panel("Duplicates received", ['sum(rate(payflow_events_processed_total{result="duplicate",%s}[1m]))' % RUN],
            legend=["duplicate/s"], unit="ops")
    d.panel("Order violations", ['sum by (kind) (increase(payflow_events_order_violations_total[5m]))'], legend=["{{kind}}"])
    return d


def latency():
    d = Dash("latency-breakdown", "Latency Breakdown")
    d.panel("Relay p99 vs consume p99", ['events:relay_p99:1m{%s}' % RUN, 'events:consume_p99:1m{%s}' % RUN],
            legend=["relay {{relay}} {{run_id}}", "consume {{run_id}}"], unit="s", w=24)
    d.panel("Relay latency heatmap", ['sum by (le) (rate(payflow_events_relay_latency_seconds_bucket{%s}[1m]))' % RUN],
            kind="heatmap", legend=["{{le}}"], extra={"options": {"calculate": False}})
    d.panel("Consume latency heatmap", ['sum by (le) (rate(payflow_events_consume_latency_seconds_bucket[1m]))'],
            kind="heatmap", legend=["{{le}}"], extra={"options": {"calculate": False}})
    d.panel("e2e p50 / p99 over time",
            ['histogram_quantile(0.5, sum by (le) (rate(payflow_events_e2e_latency_seconds_bucket{poison="false",%s}[1m])))' % RUN,
             'events:e2e_p99:1m{%s}' % RUN], legend=["p50", "p99 {{relay}}"], unit="s", w=24,
            extra={"options": {"exemplars": True}})
    return d


def relays():
    d = Dash("relays", "Relays")
    d.panel("Poller backlog rows", ['pg_outbox_backlog_rows'], legend=["backlog"], w=6, h=5, kind="stat")
    d.panel("Oldest unpublished (s)", ['pg_outbox_oldest_unpublished_seconds'], legend=["oldest"], unit="s", w=6, h=5, kind="stat")
    d.panel("Debezium ms behind source", ['debezium_metrics_millisecondsbehindsource'], legend=["behind"], unit="ms", w=12, h=5)
    d.panel("Batch size", ['rate(payflow_relay_batch_size_sum[1m]) / rate(payflow_relay_batch_size_count[1m])'], legend=["avg batch"])
    d.panel("Poll time p99", ['histogram_quantile(0.99, sum by (le) (rate(payflow_relay_poll_seconds_bucket[1m])))'], legend=["p99"], unit="s")
    d.panel("Published / s", ['rate(payflow_relay_published_total[1m])'], legend=["published"], unit="ops")
    d.panel("Debezium queue remaining capacity", ['debezium_metrics_queueremainingcapacity'], legend=["capacity"])
    d.panel("Replication slot lag bytes", ['pg_replication_slot_lag_bytes'], legend=["{{slot}}"], unit="bytes")
    d.panel("Postgres CPU (polling vs CDC)", ['container_cpu_utilization{container="postgres"}'], legend=["postgres cpu"], unit="percentunit")
    return d


def skew():
    d = Dash("partitions-skew", "Partitions & Skew")
    d.panel("Imbalance (max/mean)", ['events:partition_imbalance:1m'], kind="stat", w=6, h=5, legend=["imbalance"])
    d.panel("Hot merchant share", ['payflow_generator_hot_share'], kind="stat", unit="percentunit", w=6, h=5, legend=["hot share"])
    d.panel("Consumers in group", ['count(count by (client_id) (kafka_consumer_fetch_manager_records_consumed_rate))'], kind="stat", w=12, h=5, legend=["consumers"])
    d.panel("Per-partition produce rate", ['events:partition_rate:1m'], kind="heatmap", legend=["{{partition}}"], w=24)
    d.panel("Lag per partition (s)", ['payflow_events_consumer_lag_seconds'], legend=["p{{partition}}"], unit="s", w=24)
    return d


def consumers():
    d = Dash("consumers-rebalance", "Consumers & Rebalance")
    d.panel("Rebalances", ['sum(increase(kafka_consumer_coordinator_rebalance_total[5m]))'], kind="stat", w=6, h=5, legend=["rebalances"])
    d.panel("Longest consumption gap (s)", ['histogram_quantile(1.0, sum by (le) (rate(payflow_events_consumption_gap_seconds_bucket[1m])))'],
            kind="stat", unit="s", w=6, h=5, legend=["max gap"])
    d.panel("Dedup batch size", ['rate(payflow_events_dedup_batch_size_sum[1m]) / rate(payflow_events_dedup_batch_size_count[1m])'], legend=["avg batch"])
    d.panel("Dedup insert p99", ['histogram_quantile(0.99, sum by (le) (rate(payflow_events_dedup_insert_seconds_bucket[1m])))'], legend=["p99"], unit="s")
    d.panel("Retry and DLQ flow", ['sum by (stage) (rate(payflow_events_retry_total[1m]))'], legend=["{{stage}}"], unit="ops")
    d.panel("Rebalance latency", ['kafka_consumer_coordinator_rebalance_latency_avg'], legend=["{{client_id}}"], unit="ms")
    return d


def brokers():
    d = Dash("brokers", "Brokers")
    d.panel("Under-replicated partitions", ['sum(kafka_server_replicamanager_underreplicatedpartitions)'], kind="stat", w=6, h=5, legend=["urp"])
    d.panel("Offline partitions", ['sum(kafka_controller_kafkacontroller_offlinepartitionscount)'], kind="stat", w=6, h=5, legend=["offline"])
    d.panel("ISR shrinks", ['rate(kafka_server_replicamanager_isrshrinks_total[1m])'], legend=["shrinks/s"], w=12, h=5)
    d.panel("Bytes in / out", ['sum(rate(kafka_server_brokertopicmetrics_bytesin_total[1m]))',
                               'sum(rate(kafka_server_brokertopicmetrics_bytesout_total[1m]))'], legend=["in", "out"], unit="Bps")
    d.panel("Request handler idle %", ['kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent'], legend=["idle"], unit="percentunit")
    d.panel("Producer batch size avg", ['kafka_producer_batch_size_avg'], legend=["{{client_id}}"], unit="bytes")
    d.panel("Producer compression ratio", ['kafka_producer_compression_rate_avg'], legend=["{{client_id}}"])
    return d


def correctness():
    d = Dash("correctness", "Correctness")
    for t, m in [("Lost", "payflow_verifier_lost_events"), ("Phantom", "payflow_verifier_phantom_events"),
                 ("Duplicate effects", "payflow_verifier_duplicate_effects"), ("Order violations", "payflow_verifier_order_violations")]:
        d.panel(t, ['max by (run_id, relay) (%s{%s})' % (m, RUN)], kind="stat", w=6, h=5, legend=["{{run_id}} {{relay}}"])
    d.panel("Dual-write vs outbox: lost", ['max by (relay) (payflow_verifier_lost_events{relay=~"$relay"})'], kind="barchart", legend=["{{relay}}"], w=12)
    d.panel("Dual-write vs outbox: phantom", ['max by (relay) (payflow_verifier_phantom_events{relay=~"$relay"})'], kind="barchart", legend=["{{relay}}"], w=12)
    d.panel("Rollup mismatch (minor units)", ['max by (run_id) (payflow_rollup_mismatch_minor{%s})' % RUN], legend=["{{run_id}}"], w=12)
    d.panel("DLQ count vs pills injected", ['max by (run_id) (payflow_verifier_dlq_count{%s})' % RUN,
                                            'max by (run_id) (payflow_verifier_pills_injected{%s})' % RUN],
            legend=["dlq {{run_id}}", "pills {{run_id}}"], w=12)
    return d


if __name__ == "__main__":
    for fn in (pipeline, latency, relays, skew, consumers, brokers, correctness):
        fn().write()
    print("wrote", len(list(OUT.glob("*.json"))), "dashboards to", OUT)
