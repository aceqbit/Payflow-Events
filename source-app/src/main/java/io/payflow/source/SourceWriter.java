package io.payflow.source;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.payflow.common.EventsMetrics;
import io.payflow.common.PaymentPayload;
import io.payflow.common.Traceparent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * One transaction per event: insert the outbox row and the payments row (with the outbox-generated event id) in a
 * single statement, so the verifier can read the committed truth from payments.event_id. This class never produces
 * to Kafka (CLAUDE.md rule 1). Each event is a new payment at version 1; payments has one row per payment id.
 */
public class SourceWriter {
    static final String INSERT = """
            WITH o AS (
              INSERT INTO outbox (aggregate_type, aggregate_id, type, payload, seq, traceparent, merchant_id)
              VALUES ('payment', ?, 'PaymentAuthorized', ?::jsonb, 1, ?, ?)
              RETURNING id)
            INSERT INTO payments (id, merchant_id, amount_minor, version, event_id, run_id)
            SELECT ?, ?, ?, 1, id, ? FROM o
            RETURNING event_id""";
    static final String DELETE_OUTBOX = "DELETE FROM outbox WHERE id = ?";

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final Tracer tracer;
    private final EventsMetrics metrics;
    private final OutboxMode mode;
    private final int merchants;
    private final String runId;

    public SourceWriter(JdbcTemplate jdbc, TransactionTemplate tx, Tracer tracer, EventsMetrics metrics,
                        OutboxMode mode, int merchants, String runId) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.tracer = tracer;
        this.metrics = metrics;
        this.mode = mode;
        this.merchants = merchants;
        this.runId = runId;
    }

    public void writeOne() {
        var rnd = ThreadLocalRandom.current();
        String paymentId = UUID.randomUUID().toString();
        String merchantId = "m-%04d".formatted(rnd.nextInt(merchants));
        long amount = 100 + rnd.nextLong(100_000);
        String payload = PaymentPayload.authorized(paymentId, merchantId, amount);

        Span span = tracer.nextSpan().name("source-app.write").start();
        try (var ignored = tracer.withSpan(span)) {
            String traceparent = Traceparent.of(span.context());
            tx.executeWithoutResult(status -> {
                UUID eventId = jdbc.queryForObject(INSERT, UUID.class,
                        paymentId, payload, traceparent, merchantId,
                        paymentId, merchantId, amount, runId);
                if (mode == OutboxMode.DELETE) {
                    jdbc.update(DELETE_OUTBOX, eventId);
                }
            });
        } catch (RuntimeException e) {
            span.error(e);
            throw e;
        } finally {
            span.end();
        }
        metrics.generatorSent("uniform", 1);
    }
}
