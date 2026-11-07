package io.payflow.consumer;

import io.micrometer.core.instrument.MeterRegistry;
import io.payflow.common.EventsMetrics;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.listener.CommonContainerStoppingErrorHandler;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.transaction.support.TransactionTemplate;

/** Effectively-once consumer. Never produces business events (rule 1). */
@SpringBootApplication(scanBasePackages = "io.payflow.consumer")
public class ConsumerApp {
    public static void main(String[] args) {
        SpringApplication.run(ConsumerApp.class, args);
    }

    @Bean
    EventsMetrics eventsMetrics(MeterRegistry registry, @Value("${relay:poller}") String relay) {
        return new EventsMetrics(registry, relay);
    }

    @Bean
    EventProcessor eventProcessor(JdbcTemplate jdbc, TransactionTemplate tx, EventsMetrics metrics) {
        return new EventProcessor(jdbc, tx, metrics);
    }

    /**
     * Phase 1 failure policy until retry topics and the DLQ exist (P2-07): stop the container on any failure. Nothing is
     * skipped and nothing is retried in place on the main partition (rule 7); the uncommitted offset is redelivered on
     * restart and absorbed by the dedup insert.
     */
    @Bean
    CommonErrorHandler errorHandler() {
        return new CommonContainerStoppingErrorHandler();
    }
}
