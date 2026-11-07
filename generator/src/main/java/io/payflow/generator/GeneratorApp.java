package io.payflow.generator;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.tracing.Tracer;
import io.payflow.common.EventsMetrics;
import io.payflow.common.LoadLifecycle;
import io.payflow.common.TotalGate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/** Fixed-seed generator: rate and total from env, then it flushes metrics and exits so scripts can wait on it. */
@SpringBootApplication(scanBasePackages = "io.payflow.generator")
public class GeneratorApp {
    public static void main(String[] args) {
        SpringApplication.run(GeneratorApp.class, args);
    }

    @Bean
    EventsMetrics eventsMetrics(MeterRegistry registry, @Value("${relay:poller}") String relay) {
        return new EventsMetrics(registry, relay);
    }

    @Bean
    GeneratorWriter generatorWriter(JdbcTemplate jdbc, TransactionTemplate tx, Tracer tracer, EventsMetrics metrics,
                                    @Value("${gen.key-strategy:aggregate}") String strategy,
                                    @Value("${gen.salt-buckets:8}") int saltBuckets,
                                    @Value("${gen.merchants:1000}") int merchants,
                                    @Value("${gen.hot-share:0}") double hotShare,
                                    @Value("${gen.pill-rate:0}") double pillRate,
                                    @Value("${run.id:adhoc}") String runId, @Value("${gen.seed:42}") long seed) {
        return new GeneratorWriter(jdbc, tx, tracer, metrics, KeyStrategy.parse(strategy), saltBuckets, merchants, hotShare,
                pillRate, runId, seed);
    }

    @Bean
    LoadLifecycle load(GeneratorWriter writer, JdbcTemplate jdbc, ConfigurableApplicationContext ctx,
                       @Value("${gen.rate:1000}") int rate, @Value("${gen.workers:64}") int workers,
                       @Value("${gen.total:0}") long total, @Value("${run.id:adhoc}") String runId) {
        var gate = new TotalGate(total, () -> committed(jdbc, runId));
        return new LoadLifecycle("generator", rate, workers, () -> {
            if (!gate.claim()) {
                if (gate.firstToFinish()) {
                    TotalGate.finishAsync(ctx, "GENERATOR", () -> "committed=" + committed(jdbc, runId) + " sent=" + writer.sent()
                            + " pills=" + writer.pillsInjected() + " hot_share=" + String.format("%.4f", writer.measuredHotShare()));
                }
                return;
            }
            try {
                writer.writeOne();
            } catch (RuntimeException e) {
                gate.release();
                throw e;
            }
        });
    }

    private static long committed(JdbcTemplate jdbc, String runId) {
        Long n = jdbc.queryForObject("select count(*) from payments where run_id = ?", Long.class, runId);
        return n == null ? 0 : n;
    }
}
