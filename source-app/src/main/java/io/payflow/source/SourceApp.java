package io.payflow.source;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.tracing.Tracer;
import io.payflow.common.EventsMetrics;
import io.payflow.common.LoadLifecycle;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/** Standalone writer: business row + outbox row in one transaction, at a fixed rate. Never touches Kafka. */
@SpringBootApplication(scanBasePackages = "io.payflow.source")
public class SourceApp {
    public static void main(String[] args) {
        SpringApplication.run(SourceApp.class, args);
    }

    @Bean
    EventsMetrics eventsMetrics(MeterRegistry registry, @Value("${relay:poller}") String relay) {
        return new EventsMetrics(registry, relay);
    }

    @Bean
    SourceWriter sourceWriter(JdbcTemplate jdbc, TransactionTemplate tx, Tracer tracer, EventsMetrics metrics,
                              @Value("${outbox.mode:retain}") String mode,
                              @Value("${source.merchants:1000}") int merchants,
                              @Value("${run.id:adhoc}") String runId) {
        return new SourceWriter(jdbc, tx, tracer, metrics, OutboxMode.parse(mode), merchants, runId);
    }

    @Bean
    LoadLifecycle load(SourceWriter writer, @Value("${source.rate:1000}") int rate,
                       @Value("${source.workers:64}") int workers) {
        return new LoadLifecycle("source-app", rate, workers, writer::writeOne);
    }
}
