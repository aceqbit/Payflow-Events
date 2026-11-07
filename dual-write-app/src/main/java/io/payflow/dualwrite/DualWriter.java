package io.payflow.dualwrite;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.payflow.common.EventsMetrics;
import io.payflow.common.OutboxRow;
import io.payflow.common.PaymentPayload;
import io.payflow.common.RecordShape;
import io.payflow.common.Traceparent;
import io.payflow.common.UuidV7;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The naive dual write, for measurement only. Produces the same record shape as the relays (RecordShape) so the
 * verifier compares like with like. payments.event_id records every committed event id; the verifier reads it as truth.
 * Deliberately no retry and no reconciliation: the losses and phantoms are the result.
 */
public class DualWriter {
    private static final Logger log = LoggerFactory.getLogger(DualWriter.class);

    static final String INSERT = """
            INSERT INTO payments (id, merchant_id, amount_minor, version, event_id, run_id)
            VALUES (?, ?, ?, 1, ?, ?)""";

    /** Thrown inside the transaction to force the rollback that makes kafka-first leave a phantom. */
    static final class ForcedRollback extends RuntimeException {
        ForcedRollback() {
            super("forced rollback after kafka ack", null, false, false);
        }
    }

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final Tracer tracer;
    private final EventsMetrics metrics;
    private final Producer<String, String> producer;
    private final DualWriteMode mode;
    private final double rollbackRate;
    private final int merchants;
    private final String runId;
    private final AtomicLong sendFailures = new AtomicLong();

    public DualWriter(JdbcTemplate jdbc, TransactionTemplate tx, Tracer tracer, EventsMetrics metrics,
                      Producer<String, String> producer, DualWriteMode mode, double rollbackRate, int merchants,
                      String runId) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.tracer = tracer;
        this.metrics = metrics;
        this.producer = producer;
        this.mode = mode;
        this.rollbackRate = rollbackRate;
        this.merchants = merchants;
        this.runId = runId;
    }

    public void writeOne() throws Exception {
        var rnd = ThreadLocalRandom.current();
        String paymentId = UUID.randomUUID().toString();
        String merchantId = "m-%04d".formatted(rnd.nextInt(merchants));
        long amount = 100 + rnd.nextLong(100_000);
        UUID eventId = UuidV7.next();
        Instant occurredAt = Instant.now();
        String payload = PaymentPayload.authorized(paymentId, merchantId, amount);
        boolean forceRollback = rnd.nextDouble() < rollbackRate;

        Span span = tracer.nextSpan().name("dual-write-app." + mode.name().toLowerCase()).start();
        try (var ignored = tracer.withSpan(span)) {
            var record = RecordShape.toRecord(new OutboxRow(eventId, "payment", paymentId, "PaymentAuthorized", payload,
                    1, occurredAt, Traceparent.of(span.context()), merchantId));
            switch (mode) {
                case DB_FIRST -> dbFirst(paymentId, merchantId, amount, eventId, record);
                case KAFKA_FIRST -> kafkaFirst(paymentId, merchantId, amount, eventId, record, forceRollback);
            }
        } catch (Exception e) {
            span.error(e);
            throw e;
        } finally {
            span.end();
        }
        metrics.generatorSent("uniform", 1);
    }

    private void insert(String paymentId, String merchantId, long amount, UUID eventId) {
        jdbc.update(INSERT, paymentId, merchantId, amount, eventId, runId);
    }

    /** Commit first, then fire the send. A crash before the callback, or a failed send, loses a committed event. */
    private void dbFirst(String paymentId, String merchantId, long amount, UUID eventId,
                         ProducerRecord<String, String> record) {
        tx.executeWithoutResult(s -> insert(paymentId, merchantId, amount, eventId));
        producer.send(record, (md, ex) -> {
            if (ex != null && sendFailures.incrementAndGet() % 100 == 1) {
                log.warn("db-first send failed (event lost, count={}): {}", sendFailures.get(), ex.toString());
            }
        });
    }

    /** Wait for the broker ack, then commit. A rollback or crash after the ack leaves a phantom event in Kafka. */
    private void kafkaFirst(String paymentId, String merchantId, long amount, UUID eventId,
                            ProducerRecord<String, String> record, boolean forceRollback) throws Exception {
        producer.send(record).get();
        try {
            tx.executeWithoutResult(s -> {
                insert(paymentId, merchantId, amount, eventId);
                if (forceRollback) {
                    throw new ForcedRollback();
                }
            });
        } catch (ForcedRollback expected) {
            // The experiment: the event is already in Kafka but never committed. The verifier counts it as a phantom.
        }
    }
}
