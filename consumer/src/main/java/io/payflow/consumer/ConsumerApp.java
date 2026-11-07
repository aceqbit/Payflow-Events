package io.payflow.consumer;

import io.micrometer.core.instrument.MeterRegistry;
import io.payflow.common.EventsMetrics;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
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
    EventProcessor eventProcessor(JdbcTemplate jdbc, TransactionTemplate tx, EventsMetrics metrics,
                                  @Value("${consumer.order-check:true}") boolean orderCheck) {
        return new EventProcessor(jdbc, tx, metrics, orderCheck);
    }
}
