-- Dedup store partitioned by day (plan section 3). Drop partitions older than topic retention + 1 day.
CREATE TABLE processed_events (
    event_id     uuid        NOT NULL,
    day          date        NOT NULL DEFAULT current_date,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, day)
) PARTITION BY RANGE (day);

CREATE FUNCTION ensure_processed_events_partitions(days_ahead int DEFAULT 7) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE d date;
BEGIN
    FOR d IN SELECT generate_series(current_date - 1, current_date + days_ahead, interval '1 day')::date LOOP
        EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF processed_events FOR VALUES FROM (%L) TO (%L)',
                       'processed_events_' || to_char(d, 'YYYYMMDD'), d, d + 1);
    END LOOP;
END $$;

CREATE FUNCTION drop_old_processed_events(keep_days int DEFAULT 8) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE r record; n int := 0;
BEGIN
    FOR r IN SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
             WHERE i.inhparent = 'processed_events'::regclass
               AND to_date(substr(c.relname, length('processed_events_') + 1), 'YYYYMMDD') < current_date - keep_days LOOP
        EXECUTE format('DROP TABLE %I', r.relname);
        n := n + 1;
    END LOOP;
    RETURN n;
END $$;

SELECT ensure_processed_events_partitions(7);
