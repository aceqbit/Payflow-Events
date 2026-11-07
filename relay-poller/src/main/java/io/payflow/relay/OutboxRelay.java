package io.payflow.relay;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import io.payflow.common.EventsMetrics;
import io.payflow.common.OutboxRow;
import io.payflow.common.RecordShape;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Relay A, the polling publisher (plan section 3): select a batch with FOR UPDATE SKIP LOCKED, send it with the
 * idempotent producer, wait for every ack, mark published_at, commit. A crash between the acks and the commit re-sends
 * the batch; consumers dedupe on the id header. Any send failure rolls the batch back untouched.
 */
public class OutboxRelay {
    static final String SELECT = """
            SELECT id, aggregate_type, aggregate_id, type, payload::text AS payload, seq, occurred_at, traceparent,
                   merchant_id
            FROM outbox
            WHERE published_at IS NULL
            ORDER BY id
            LIMIT ?
            FOR UPDATE SKIP LOCKED""";
    static final String MARK = "UPDATE outbox SET published_at = now() WHERE id = ANY(?::uuid[])";

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final Producer<String, String> producer;
    private final EventsMetrics metrics;
    private final int batch;

    public OutboxRelay(JdbcTemplate jdbc, TransactionTemplate tx, Producer<String, String> producer,
                       EventsMetrics metrics, int batch) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.producer = producer;
        this.metrics = metrics;
        this.batch = batch;
    }

    public int batchSize() {
        return batch;
    }

    /** One poll. Returns the number of rows published (0 when the outbox had nothing unpublished and unlocked). */
    public int pollOnce() {
        long t0 = System.nanoTime();
        Integer n = tx.execute(status -> {
            List<OutboxRow> rows = jdbc.query(SELECT, (rs, i) -> new OutboxRow(
                    rs.getObject("id", UUID.class), rs.getString("aggregate_type"), rs.getString("aggregate_id"),
                    rs.getString("type"), rs.getString("payload"), rs.getLong("seq"),
                    rs.getObject("occurred_at", OffsetDateTime.class).toInstant(), rs.getString("traceparent"),
                    rs.getString("merchant_id")), batch);
            if (rows.isEmpty()) {
                return 0;
            }
            List<Future<RecordMetadata>> acks = new ArrayList<>(rows.size());
            for (OutboxRow r : rows) {
                acks.add(producer.send(RecordShape.toRecord(r)));
            }
            awaitAll(acks);
            String ids = "{" + String.join(",", rows.stream().map(r -> r.id().toString()).toList()) + "}";
            jdbc.update(MARK, ids);
            return rows.size();
        });
        int published = n == null ? 0 : n;
        if (published > 0) {
            metrics.relayBatch(published, Duration.ofNanos(System.nanoTime() - t0));
            metrics.relayPublished(published);
        }
        return published;
    }

    private static void awaitAll(List<Future<RecordMetadata>> acks) {
        try {
            for (Future<RecordMetadata> f : acks) {
                f.get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting for broker acks", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("broker did not ack the batch; rolling it back", e.getCause());
        }
    }
}
