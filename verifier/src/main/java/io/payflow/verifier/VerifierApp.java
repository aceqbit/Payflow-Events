package io.payflow.verifier;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import io.micrometer.core.instrument.MeterRegistry;
import io.payflow.common.EventsMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

/**
 * CLI and scheduled job. One verification for RUN_ID, or with VERIFY_EVERY_SECONDS > 0 a loop that re-verifies and
 * refreshes the gauges (so VerifierStale can fire when it stops). Exit code 1 when the run is not clean and
 * VERIFY_EXPECT_CLEAN=true.
 */
@SpringBootApplication(scanBasePackages = "io.payflow.verifier")
public class VerifierApp {
    private static final Logger log = LoggerFactory.getLogger(VerifierApp.class);
    static volatile int exitCode = 0;

    public static void main(String[] args) {
        var ctx = SpringApplication.run(VerifierApp.class, args);
        int code = SpringApplication.exit(ctx, () -> exitCode);
        System.exit(code);
    }

    @Bean
    EventsMetrics eventsMetrics(MeterRegistry registry, @Value("${relay:poller}") String relay) {
        return new EventsMetrics(registry, relay);
    }

    @Bean
    CommandLineRunner verify(JdbcTemplate jdbc, EventsMetrics metrics,
                             @Value("${verify.run-id:adhoc}") String runId, @Value("${verify.scenario:none}") String scenario,
                             @Value("${relay:poller}") String relay, @Value("${kafka.bootstrap}") String bootstrap,
                             @Value("${verify.topic:payment.events}") String topic,
                             @Value("${verify.dlq-topic:payments.dlq}") String dlqTopic,
                             @Value("${verify.totals-topic:merchant.totals}") String totalsTopic,
                             @Value("${verify.check-rollup:false}") boolean checkRollup,
                             @Value("${verify.start-offsets:}") String startOffsetsJson,
                             @Value("${verify.results-file:docs/results.json}") String resultsFile,
                             @Value("${verify.every-seconds:0}") int everySeconds,
                             @Value("${verify.expect-clean:false}") boolean expectClean,
                             @Value("${verify.window-start:0}") long windowStart, @Value("${verify.window-end:0}") long windowEnd,
                             @Value("${verify.rate:0}") long rate, @Value("${verify.bench-name:}") String benchName) {
        return args -> {
            var scanner = new KafkaScanner(jdbc, bootstrap);
            var verification = new Verification(jdbc);
            Map<Integer, Long> start = parseOffsets(startOffsetsJson);
            do {
                long scanned = scanner.scanEvents(runId, topic, start);
                long dlq = scanner.scanDlq(runId, dlqTopic);
                if (checkRollup) {
                    scanner.scanTotals(runId, totalsTopic);
                }
                var r = verification.verify(runId, checkRollup);
                publish(metrics, r, runId, scenario, relay);
                Map<String, Object> extra = new java.util.LinkedHashMap<>();
                extra.put("kafka_records", scanned);
                if (windowStart > 0 && windowEnd > windowStart) {
                    extra.put("run_start_epoch", windowStart);
                    extra.put("run_end_epoch", windowEnd);
                }
                if (rate > 0) {
                    extra.put("offered_rate", rate);
                }
                if (!benchName.isBlank()) {
                    extra.put("bench", benchName);
                }
                ResultsFile.append(Path.of(resultsFile), runId, scenario, relay, r, extra);
                log.info("VERIFY run={} scenario={} relay={} {} kafka_records={} dlq_records={}", runId, scenario, relay, r,
                        scanned, dlq);
                System.out.println("VERIFY " + runId + " " + r);
                exitCode = expectClean && !r.outboxClean() ? 1 : 0;
                if (everySeconds > 0) {
                    Thread.sleep(everySeconds * 1000L);
                }
            } while (everySeconds > 0);
            Thread.sleep(6_000); // let the 5 s OTLP metric push carry the gauges out before the JVM exits
        };
    }

    /** Names exactly as plan section 5: payflow_verifier_*_events / _duplicate_effects / ... and payflow_rollup_mismatch_minor. */
    static void publish(EventsMetrics m, Verification.Result r, String runId, String scenario, String relay) {
        m.verifierGauge("lost.events", r.lost(), runId, scenario, relay);
        m.verifierGauge("phantom.events", r.phantom(), runId, scenario, relay);
        m.verifierGauge("duplicate.effects", r.duplicateEffects(), runId, scenario, relay);
        m.verifierGauge("duplicates.received", r.duplicatesReceived(), runId, scenario, relay);
        m.verifierGauge("order.violations", r.orderViolations(), runId, scenario, relay);
        m.verifierGauge("dlq.count", r.dlqCount(), runId, scenario, relay);
        m.verifierGauge("pills.injected", r.pillsInjected(), runId, scenario, relay);
        m.verifierGauge("last.run.timestamp.seconds", System.currentTimeMillis() / 1000.0, runId, scenario, relay);
        m.gauge("payflow.rollup.mismatch.minor", r.rollupMismatchMinor(), runId, scenario, relay);
    }

    static Map<Integer, Long> parseOffsets(String json) {
        if (json == null || json.isBlank()) {
            return Map.of();
        }
        Map<String, Number> raw = JsonMapper.builder().build().readValue(json, new TypeReference<Map<String, Number>>() { });
        Map<Integer, Long> out = new HashMap<>();
        raw.forEach((k, v) -> out.put(Integer.parseInt(k), v.longValue()));
        return out;
    }
}
