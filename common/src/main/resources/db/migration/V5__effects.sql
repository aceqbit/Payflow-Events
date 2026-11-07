-- Consumer side effect. effect_log has no uniqueness on event_id on purpose: the verifier counts duplicates there.
CREATE TABLE merchant_balance (
    merchant_id   text   PRIMARY KEY,
    balance_minor bigint NOT NULL DEFAULT 0
);
CREATE TABLE effect_log (
    event_id   uuid        NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX effect_log_event_id_idx ON effect_log (event_id);
