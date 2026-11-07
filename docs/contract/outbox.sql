-- Outbox contract owned by PayFlow Ledger (V5__outbox.sql). Do not change here.
CREATE TABLE outbox (
    id             uuid        PRIMARY KEY DEFAULT uuidv7(),
    aggregate_type text        NOT NULL,
    aggregate_id   text        NOT NULL,
    type           text        NOT NULL,
    payload        jsonb       NOT NULL,
    seq            bigint      NOT NULL,
    occurred_at    timestamptz NOT NULL DEFAULT now(),
    traceparent    text,
    merchant_id    text        NOT NULL,
    published_at   timestamptz
);
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;
