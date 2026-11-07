package io.payflow.consumer;

import java.time.Duration;
import java.time.Instant;

import io.payflow.common.EventMeta;
import io.payflow.common.EventsMetrics;
import io.payflow.common.RecordShape;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Manual-ack listener. The offset is acknowledged only after the DB transaction committed (rule 4). Exceptions are not
 * caught: they reach the container error handler (rule 7, "do not swallow").
 * Latency split (CLAUDE.md): relay = record timestamp (LogAppendTime) - occurred_at; consume = now after commit - record
 * timestamp; e2e is their sum inside EventsMetrics.recordLatency.
 */
@Component
public class PaymentListener {
    private final EventProcessor processor;
    private final EventsMetrics metrics;

    public PaymentListener(EventProcessor processor, EventsMetrics metrics) {
        this.processor = processor;
        this.metrics = metrics;
    }

    @KafkaListener(topics = "${consumer.topic:payment.events}", groupId = "${consumer.group:payflow-consumer}",
            concurrency = "${consumer.concurrency:3}")
    public void onRecord(ConsumerRecord<String, String> rec, Acknowledgment ack) {
        EventProcessor.Result result;
        EventMeta meta;
        try {
            meta = RecordShape.fromRecord(rec);
            result = processor.process(meta);
        } catch (RuntimeException e) {
            metrics.retry(nextStage(rec.topic(), e)); // count where the failure goes, then let the retry/DLQ path see it
            throw e;
        }
        ack.acknowledge();

        Instant now = Instant.now();
        Instant appended = Instant.ofEpochMilli(rec.timestamp());
        if (result == EventProcessor.Result.APPLIED) {
            metrics.recordLatency(rec.topic(), false, nonNegative(Duration.between(meta.occurredAt(), appended)),
                    nonNegative(Duration.between(appended, now)));
        }
        metrics.consumerLag(rec.topic(), rec.partition(), nonNegative(Duration.between(appended, now)).toMillis() / 1000.0);
    }

    /** Where a failed record goes next: retry-5s, retry-1m or the DLQ (non-retryable errors skip straight to the DLQ). */
    static String nextStage(String topic, RuntimeException e) {
        if (e instanceof io.payflow.common.NonRetryableEventException || topic.endsWith("-retry-1m")) {
            return "dlq";
        }
        return topic.endsWith("-retry-5s") ? "retry-1m" : "retry-5s";
    }

    /** A partition with nothing left to read has no lag. */
    @EventListener
    public void onIdle(ListenerContainerIdleEvent event) {
        event.getTopicPartitions().forEach(tp -> metrics.consumerLag(tp.topic(), tp.partition(), 0));
    }

    private static Duration nonNegative(Duration d) {
        return d.isNegative() ? Duration.ZERO : d;
    }
}
