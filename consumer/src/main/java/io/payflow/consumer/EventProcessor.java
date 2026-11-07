package io.payflow.consumer;

import java.util.List;

import io.payflow.common.EventMeta;
import io.payflow.common.EventsMetrics;
import io.payflow.common.NonRetryableEventException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Phase 1 baseline of the effectively-once consumer (plan section 3): per event, ONE Postgres transaction that holds the
 * dedup insert (ON CONFLICT DO NOTHING), the seq check, the side effect and the seq update (CLAUDE.md rules 4 and 5).
 * The caller commits the Kafka offset only after process() returns.
 */
public class EventProcessor {
    public enum Result {
        /** First delivery, effect applied. */
        APPLIED("applied"),
        /** The id was already processed: no effect. */
        DUPLICATE("duplicate"),
        /** New id but its seq is not newer than the aggregate's last seq: counted, never applied. */
        REVERSAL_SKIPPED("reversal_skipped");

        private final String label;

        Result(String label) {
            this.label = label;
        }

        public String label() {
            return label;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(EventProcessor.class);
    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    static final String DEDUP = "INSERT INTO processed_events (event_id) VALUES (?) ON CONFLICT DO NOTHING";
    static final String LAST_SEQ = "SELECT last_seq FROM aggregate_seq WHERE aggregate_id = ? FOR UPDATE";
    static final String BALANCE = """
            INSERT INTO merchant_balance (merchant_id, balance_minor) VALUES (?, ?)
            ON CONFLICT (merchant_id) DO UPDATE SET balance_minor = merchant_balance.balance_minor + EXCLUDED.balance_minor""";
    static final String EFFECT = "INSERT INTO effect_log (event_id) VALUES (?)";
    static final String SET_SEQ = """
            INSERT INTO aggregate_seq (aggregate_id, last_seq) VALUES (?, ?)
            ON CONFLICT (aggregate_id) DO UPDATE SET last_seq = EXCLUDED.last_seq""";

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final EventsMetrics metrics;

    public EventProcessor(JdbcTemplate jdbc, TransactionTemplate tx, EventsMetrics metrics) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.metrics = metrics;
    }

    public Result process(EventMeta m) {
        long delta = delta(m);
        long t0 = System.nanoTime();
        Result r = tx.execute(status -> {
            int inserted = jdbc.update(DEDUP, m.id());
            metrics.dedupInsertTimer().record(System.nanoTime() - t0, java.util.concurrent.TimeUnit.NANOSECONDS);
            if (inserted == 0) {
                return Result.DUPLICATE;
            }
            List<Long> last = jdbc.queryForList(LAST_SEQ, Long.class, m.aggregateId());
            long expected = last.isEmpty() ? 1 : last.get(0) + 1;
            boolean reversal = !last.isEmpty() && m.seq() <= last.get(0);
            if (reversal) {
                violation("reversal", m, expected);
                return Result.REVERSAL_SKIPPED;
            }
            if (m.seq() != expected) {
                violation("gap", m, expected);
            }
            jdbc.update(BALANCE, m.merchantId(), delta);
            jdbc.update(EFFECT, m.id());
            jdbc.update(SET_SEQ, m.aggregateId(), m.seq());
            return Result.APPLIED;
        });
        metrics.processed(m.topic(), r.label(), 1);
        return r;
    }

    private void violation(String kind, EventMeta m, long expected) {
        metrics.orderViolation(kind);
        log.warn("order violation kind={} aggregate={} seq={} expected={} id={} trace={}", kind, m.aggregateId(), m.seq(),
                expected, m.id(), traceId(m.traceparent()));
    }

    /** Trace id out of a W3C traceparent, or "-" when the event carried none. */
    static String traceId(String traceparent) {
        if (traceparent == null) {
            return "-";
        }
        String[] p = traceparent.split("-");
        return p.length >= 2 ? p[1] : "-";
    }

    /** Signed amount of the event. A payload we cannot read is not retryable (rule 7). */
    static long delta(EventMeta m) {
        try {
            JsonNode n = MAPPER.readTree(m.payload());
            if (!n.has("amount_minor") || !n.get("amount_minor").isIntegralNumber()) {
                throw new NonRetryableEventException("payload has no integer amount_minor, id=" + m.id());
            }
            long amount = n.get("amount_minor").asLong();
            return "PaymentRefunded".equals(m.type()) ? -amount : amount;
        } catch (NonRetryableEventException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new NonRetryableEventException("unreadable payload, id=" + m.id() + ": " + e.getMessage());
        }
    }
}
