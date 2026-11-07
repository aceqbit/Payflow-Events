package io.payflow.common;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class RecordShapeTest {
    private final OutboxRow row = new OutboxRow(UUID.randomUUID(), "payment", "pay-1", "PaymentAuthorized",
            "{\"payment_id\": \"pay-1\", \"amount_minor\": 100, \"merchant_id\": \"m1\"}", 3,
            Instant.parse("2026-11-07T10:00:00.123Z"), "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", "m1");

    @Test
    void roundTrip() {
        var rec = RecordShape.toRecord(row);
        assertThat(rec.topic()).isEqualTo("payment.events");
        assertThat(rec.key()).isEqualTo("pay-1");
        assertThat(rec.value()).doesNotContain(": ").doesNotContain(", ");

        var cr = new ConsumerRecord<>(rec.topic(), 0, 0L, rec.key(), rec.value());
        rec.headers().forEach(h -> cr.headers().add(h));
        var meta = RecordShape.fromRecord(cr);
        assertThat(meta.id()).isEqualTo(row.id());
        assertThat(meta.seq()).isEqualTo(3);
        assertThat(meta.occurredAt()).isEqualTo(row.occurredAt());
        assertThat(meta.traceparent()).isEqualTo(row.traceparent());
        assertThat(meta.merchantId()).isEqualTo("m1");
    }

    @Test
    void headersAreExactlyThePlanSet() {
        var rec = RecordShape.toRecord(row);
        var names = new java.util.ArrayList<String>();
        rec.headers().forEach(h -> names.add(h.key()));
        assertThat(names).containsExactlyInAnyOrder("id", "type", "seq", "occurred_at", "traceparent", "merchant_id");
        assertThat(new String(rec.headers().lastHeader("id").value(), StandardCharsets.UTF_8)).isEqualTo(row.id().toString());
    }

    @Test
    void saltingIsStableAndBounded() {
        for (int i = 0; i < 100; i++) {
            int b = Salting.bucket("p" + i, 8);
            assertThat(b).isBetween(0, 7);
            assertThat(Salting.bucket("p" + i, 8)).isEqualTo(b);
        }
        assertThat(Salting.merchantOf(Salting.stage1Key("m9", "p1", 8))).isEqualTo("m9");
    }
}
