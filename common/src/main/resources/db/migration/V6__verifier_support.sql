-- Verifier staging and bookkeeping (plan section 7). Kafka is scanned into verifier_kafka so every comparison is a SQL
-- anti-join instead of a Java HashSet.
CREATE TABLE verifier_kafka (
    run_id   text   NOT NULL,
    topic    text   NOT NULL,
    part     int    NOT NULL,
    off      bigint NOT NULL,
    event_id uuid   NOT NULL,
    agg_key  text   NOT NULL,
    seq      bigint NOT NULL,
    PRIMARY KEY (run_id, topic, part, off)
);
CREATE INDEX verifier_kafka_event_idx ON verifier_kafka (run_id, event_id);

-- dead-lettered event ids (may repeat when the DLQ is replayed and fails again)
CREATE TABLE verifier_dlq (
    run_id   text NOT NULL,
    event_id uuid NOT NULL
);
CREATE INDEX verifier_dlq_idx ON verifier_dlq (run_id, event_id);

-- latest merchant.totals value per merchant for the run
CREATE TABLE verifier_totals (
    run_id      text   NOT NULL,
    merchant_id text   NOT NULL,
    total_minor bigint NOT NULL,
    PRIMARY KEY (run_id, merchant_id)
);

-- events the generator deliberately poisoned (counted, not hidden)
CREATE TABLE pills (
    run_id   text NOT NULL,
    event_id uuid NOT NULL,
    PRIMARY KEY (run_id, event_id)
);
