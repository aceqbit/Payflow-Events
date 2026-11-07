package io.payflow.generator;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.utils.Utils;

/**
 * Which value becomes the outbox aggregate_id, and therefore the Kafka partition key. AGGREGATE is the production rule
 * (CLAUDE.md rule 6: key = entity). MERCHANT and SALTED exist only for the skew lab experiment (plan section 8).
 */
public enum KeyStrategy {
    AGGREGATE, MERCHANT, SALTED;

    public static KeyStrategy parse(String s) {
        return switch (s.toLowerCase()) {
            case "aggregate", "aggregate_id", "payment" -> AGGREGATE;
            case "merchant", "merchant_id" -> MERCHANT;
            case "salted" -> SALTED;
            default -> throw new IllegalArgumentException("key.strategy must be aggregate|merchant|salted, got " + s);
        };
    }

    /** aggregate_id for an event. The salt is murmur2(payment_id) % buckets, the same hash stage 1 of the rollup uses. */
    public String aggregateId(String paymentId, String merchantId, int saltBuckets) {
        return switch (this) {
            case AGGREGATE -> paymentId;
            case MERCHANT -> merchantId;
            case SALTED -> merchantId + "#" + salt(paymentId, saltBuckets);
        };
    }

    public static int salt(String paymentId, int buckets) {
        return Utils.toPositive(Utils.murmur2(paymentId.getBytes(StandardCharsets.UTF_8))) % buckets;
    }
}
