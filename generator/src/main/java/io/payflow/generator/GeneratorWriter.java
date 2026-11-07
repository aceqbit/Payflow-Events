package io.payflow.generator;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.payflow.common.EventsMetrics;
import io.payflow.common.PaymentPayload;
import io.payflow.common.Traceparent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Deterministic load generator. Like source-app it only writes the business row and its outbox row in one transaction;
 * it never produces to Kafka (rule 1). Differences: a fixed seed, an optional hot merchant, a key strategy switch for the
 * skew lab, and deliberate poison pills that are recorded in the pills table so the verifier can count them.
 */
public class GeneratorWriter {
    static final String INSERT = """
            WITH o AS (
              INSERT INTO outbox (aggregate_type, aggregate_id, type, payload, seq, traceparent, merchant_id)
              VALUES ('payment', ?, 'PaymentAuthorized', ?::jsonb, 1, ?, ?)
              RETURNING id)
            INSERT INTO payments (id, merchant_id, amount_minor, version, event_id, run_id)
            SELECT ?, ?, ?, 1, id, ? FROM o
            RETURNING event_id""";
    static final String PILL = "INSERT INTO pills (run_id, event_id) VALUES (?, ?)";

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final Tracer tracer;
    private final EventsMetrics metrics;
    private final KeyStrategy keyStrategy;
    private final int saltBuckets;
    private final int merchants;
    private final double hotShare;
    private final double pillRate;
    private final String runId;
    private final String strategyLabel;
    private final long seed;
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong hot = new AtomicLong();
    private final AtomicLong pills = new AtomicLong();
    private final AtomicLong hotShareBps;
    private final ThreadLocal<java.util.SplittableRandom> rnd;

    public GeneratorWriter(JdbcTemplate jdbc, TransactionTemplate tx, Tracer tracer, EventsMetrics metrics,
                           KeyStrategy keyStrategy, int saltBuckets, int merchants, double hotShare, double pillRate,
                           String runId, long seed) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.tracer = tracer;
        this.metrics = metrics;
        this.keyStrategy = keyStrategy;
        this.saltBuckets = saltBuckets;
        this.merchants = merchants;
        this.hotShare = hotShare;
        this.pillRate = pillRate;
        this.runId = runId;
        this.seed = seed;
        this.strategyLabel = keyStrategy.name().toLowerCase();
        this.hotShareBps = metrics.hotShare();
        var counter = new AtomicLong();
        this.rnd = ThreadLocal.withInitial(() -> new java.util.SplittableRandom(seed * 1_000_003L + counter.getAndIncrement()));
    }

    /** One event. Deterministic given the seed and the order in which threads draw their streams. */
    public void writeOne() {
        var r = rnd.get();
        String paymentId = new UUID(r.nextLong(), r.nextLong()).toString();
        boolean isHot = hotShare > 0 && r.nextDouble() < hotShare;
        String merchantId = isHot ? "m-0000" : "m-%04d".formatted(1 + r.nextInt(Math.max(1, merchants - 1)));
        long amount = 100 + r.nextLong(100_000);
        boolean pill = pillRate > 0 && r.nextDouble() < pillRate;
        boolean malformed = pill && r.nextBoolean();
        String payload = pill ? poisonPayload(paymentId, merchantId, amount, malformed)
                : PaymentPayload.authorized(paymentId, merchantId, amount);
        String aggregateId = keyStrategy.aggregateId(paymentId, merchantId, saltBuckets);

        Span span = tracer.nextSpan().name("generator.write").start();
        try (var ignored = tracer.withSpan(span)) {
            String traceparent = Traceparent.of(span.context());
            tx.executeWithoutResult(status -> {
                UUID eventId = jdbc.queryForObject(INSERT, UUID.class, aggregateId, payload, traceparent, merchantId,
                        paymentId, merchantId, amount, runId);
                if (pill) {
                    jdbc.update(PILL, runId, eventId);
                }
            });
        } catch (RuntimeException e) {
            span.error(e);
            throw e;
        } finally {
            span.end();
        }
        long n = sent.incrementAndGet();
        if (isHot) {
            hot.incrementAndGet();
        }
        if (pill) {
            pills.incrementAndGet();
        }
        metrics.generatorSent(strategyLabel, 1);
        hotShareBps.set(hot.get() * 10_000 / n);
    }

    /**
     * Pills stay valid JSON (the outbox column is jsonb) but cannot be applied: malformed drops the integer amount so the
     * consumer rejects it as non-retryable; the flagged kind is well formed and makes the side effect throw (retryable).
     */
    static String poisonPayload(String paymentId, String merchantId, long amount, boolean malformed) {
        if (malformed) {
            return "{\"payment_id\":\"" + paymentId + "\",\"merchant_id\":\"" + merchantId
                    + "\",\"amount_minor\":\"not-a-number\",\"currency\":\"USD\",\"poison\":true}";
        }
        return "{\"payment_id\":\"" + paymentId + "\",\"merchant_id\":\"" + merchantId + "\",\"amount_minor\":" + amount
                + ",\"currency\":\"USD\",\"poison\":true,\"fail_effect\":true}";
    }

    public long sent() {
        return sent.get();
    }

    public long pillsInjected() {
        return pills.get();
    }

    public double measuredHotShare() {
        long n = sent.get();
        return n == 0 ? 0 : (double) hot.get() / n;
    }
}
