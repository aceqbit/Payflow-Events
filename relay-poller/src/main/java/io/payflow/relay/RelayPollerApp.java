package io.payflow.relay;

import java.util.Properties;

import io.micrometer.core.instrument.MeterRegistry;
import io.payflow.common.EventsMetrics;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/** Relay A. The only place besides the DLQ replay CLI that produces business events (CLAUDE.md rule 1). */
@SpringBootApplication(scanBasePackages = "io.payflow.relay")
public class RelayPollerApp {
    public static void main(String[] args) {
        SpringApplication.run(RelayPollerApp.class, args);
    }

    @Bean
    EventsMetrics eventsMetrics(MeterRegistry registry, @Value("${relay:poller}") String relay) {
        return new EventsMetrics(registry, relay);
    }

    /** Rule 3: idempotence on, acks=all. Never lower these. */
    @Bean(destroyMethod = "close")
    Producer<String, String> producer(
            @Value("${kafka.bootstrap:kafka-1:9092,kafka-2:9092,kafka-3:9092}") String bootstrap,
            @Value("${relay.linger-ms:5}") int lingerMs) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        p.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(p);
    }

    @Bean
    OutboxRelay outboxRelay(JdbcTemplate jdbc, TransactionTemplate tx, Producer<String, String> producer,
                            EventsMetrics metrics, @Value("${relay.batch:500}") int batch) {
        return new OutboxRelay(jdbc, tx, producer, metrics, batch);
    }

    @Bean
    RelayLoop relayLoop(OutboxRelay relay, @Value("${relay.busy-backoff-ms:5}") long busyMs,
                        @Value("${relay.idle-backoff-ms:50}") long idleMs) {
        return new RelayLoop(relay, busyMs, idleMs);
    }
}
