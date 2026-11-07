package io.payflow.common;

import java.time.Instant;
import java.util.UUID;

/** Decoded view of a consumed record. */
public record EventMeta(UUID id, String type, long seq, Instant occurredAt, String traceparent, String merchantId,
                        String aggregateId, String payload, long appendTimeMs, String topic, int partition) {
}
