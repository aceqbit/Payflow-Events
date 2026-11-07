package io.payflow.common;

import java.time.Instant;
import java.util.UUID;

/** One row of the Ledger outbox contract. payload is the jsonb text. */
public record OutboxRow(UUID id, String aggregateType, String aggregateId, String type, String payload,
                        long seq, Instant occurredAt, String traceparent, String merchantId) {
}
