-- Source business table. event_id records the committed event id so the verifier can read the truth.
CREATE TABLE payments (
    id           text        PRIMARY KEY,
    merchant_id  text        NOT NULL,
    amount_minor bigint      NOT NULL,
    version      bigint      NOT NULL,
    event_id     uuid        NOT NULL,
    run_id       text        NOT NULL DEFAULT 'adhoc',
    created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX payments_event_id_idx ON payments (event_id);
