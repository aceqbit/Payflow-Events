package io.payflow.common;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

import tools.jackson.databind.json.JsonMapper;

/**
 * Single definition of the record both relays must produce (plan section 3):
 * key=aggregate_id, value=payload as compact JSON, headers id,type,seq,occurred_at,traceparent,merchant_id.
 * The payload is re-serialised compactly because the Debezium JsonConverter emits compact JSON while jsonb text
 * carries spaces after separators.
 */
public final class RecordShape {
    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    private RecordShape() {
    }

    public static String topic(String aggregateType) {
        return aggregateType + ".events";
    }

    public static String compact(String json) {
        return MAPPER.writeValueAsString(MAPPER.readTree(json));
    }

    public static ProducerRecord<String, String> toRecord(OutboxRow r) {
        var rec = new ProducerRecord<String, String>(topic(r.aggregateType()), r.aggregateId(), compact(r.payload()));
        add(rec, Headers.ID, r.id().toString());
        add(rec, Headers.TYPE, r.type());
        add(rec, Headers.SEQ, Long.toString(r.seq()));
        add(rec, Headers.OCCURRED_AT, r.occurredAt().toString());
        if (r.traceparent() != null) {
            add(rec, Headers.TRACEPARENT, r.traceparent());
        }
        add(rec, Headers.MERCHANT_ID, r.merchantId());
        return rec;
    }

    public static EventMeta fromRecord(ConsumerRecord<String, String> rec) {
        return new EventMeta(UUID.fromString(required(rec, Headers.ID)), required(rec, Headers.TYPE),
                Long.parseLong(required(rec, Headers.SEQ)), Instant.parse(required(rec, Headers.OCCURRED_AT)),
                header(rec, Headers.TRACEPARENT), required(rec, Headers.MERCHANT_ID), rec.key(), rec.value(),
                rec.timestamp(), rec.topic(), rec.partition());
    }

    public static String header(ConsumerRecord<?, ?> rec, Headers h) {
        Header hdr = rec.headers().lastHeader(h.key());
        return hdr == null ? null : new String(hdr.value(), StandardCharsets.UTF_8);
    }

    private static String required(ConsumerRecord<?, ?> rec, Headers h) {
        String v = header(rec, h);
        if (v == null) {
            throw new NonRetryableEventException("missing header " + h.key());
        }
        return v;
    }

    private static void add(ProducerRecord<String, String> rec, Headers h, String v) {
        rec.headers().add(h.key(), v.getBytes(StandardCharsets.UTF_8));
    }
}
