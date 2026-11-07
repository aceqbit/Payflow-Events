package io.payflow.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.payflow.common.EventMeta;
import io.payflow.common.EventsMetrics;
import io.payflow.common.NonRetryableEventException;
import io.payflow.common.UuidV7;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class EventProcessorIT {
    @Container
    static PostgreSQLContainer<?> pg = new PostgreSQLContainer<>("postgres:18");

    static JdbcTemplate jdbc;
    static TransactionTemplate tx;
    SimpleMeterRegistry reg;
    EventProcessor processor;

    @BeforeAll
    static void migrate() {
        Flyway.configure().dataSource(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword())
                .locations("classpath:db/migration").load().migrate();
        var ds = new DriverManagerDataSource(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
        jdbc = new JdbcTemplate(ds);
        tx = new TransactionTemplate(new DataSourceTransactionManager(ds));
    }

    @BeforeEach
    void reset() {
        jdbc.execute("TRUNCATE processed_events, effect_log, aggregate_seq, merchant_balance");
        reg = new SimpleMeterRegistry();
        processor = new EventProcessor(jdbc, tx, new EventsMetrics(reg, "poller"));
    }

    private static EventMeta event(UUID id, String aggregate, long seq, long amount, String type) {
        return new EventMeta(id, type, seq, Instant.now(), "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                "m-1", aggregate, "{\"payment_id\":\"" + aggregate + "\",\"amount_minor\":" + amount + "}", 0,
                "payment.events", 0);
    }

    private double count(String name, String tagKey, String tagValue) {
        var c = reg.find(name).tag(tagKey, tagValue).counter();
        return c == null ? 0 : c.count();
    }

    private int rows(String table) {
        return jdbc.queryForObject("select count(*) from " + table, Integer.class);
    }

    /** Re-publish the same 10k events 3 times: 10k effects, 20k duplicates counted. */
    @Test
    void tenThousandEventsDeliveredThreeTimesGiveTenThousandEffects() {
        List<EventMeta> events = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) {
            events.add(event(UuidV7.next(), "p-" + i, 1, 10, "PaymentAuthorized"));
        }
        for (int round = 0; round < 3; round++) {
            events.forEach(processor::process);
        }
        assertThat(rows("effect_log")).isEqualTo(10_000);
        assertThat(jdbc.queryForObject("select count(distinct event_id) from effect_log", Integer.class))
                .isEqualTo(10_000);
        assertThat(rows("processed_events")).isEqualTo(10_000);
        assertThat(jdbc.queryForObject("select balance_minor from merchant_balance where merchant_id='m-1'", Long.class))
                .isEqualTo(100_000L);
        assertThat(count("payflow.events.processed", "result", "applied")).isEqualTo(10_000);
        assertThat(count("payflow.events.processed", "result", "duplicate")).isEqualTo(20_000);
        assertThat(reg.find("payflow.events.order.violations").counters()).isEmpty();
    }

    @Test
    void reversalIsCountedLoggedAndNeverApplied() {
        processor.process(event(UuidV7.next(), "p-1", 1, 100, "PaymentAuthorized"));
        processor.process(event(UuidV7.next(), "p-1", 2, 50, "PaymentCaptured"));
        // a different event id carrying an older seq: not a duplicate, a reversal
        var r = processor.process(event(UuidV7.next(), "p-1", 1, 999, "PaymentAuthorized"));

        assertThat(r).isEqualTo(EventProcessor.Result.REVERSAL_SKIPPED);
        assertThat(count("payflow.events.order.violations", "kind", "reversal")).isEqualTo(1);
        assertThat(rows("effect_log")).isEqualTo(2);
        assertThat(jdbc.queryForObject("select balance_minor from merchant_balance", Long.class)).isEqualTo(150L);
        assertThat(jdbc.queryForObject("select last_seq from aggregate_seq where aggregate_id='p-1'", Long.class))
                .isEqualTo(2L);
    }

    @Test
    void gapIsCountedButTheEventIsApplied() {
        processor.process(event(UuidV7.next(), "p-2", 1, 10, "PaymentAuthorized"));
        var r = processor.process(event(UuidV7.next(), "p-2", 3, 10, "PaymentRefunded"));

        assertThat(r).isEqualTo(EventProcessor.Result.APPLIED);
        assertThat(count("payflow.events.order.violations", "kind", "gap")).isEqualTo(1);
        assertThat(jdbc.queryForObject("select balance_minor from merchant_balance", Long.class)).isEqualTo(0L);
        assertThat(jdbc.queryForObject("select last_seq from aggregate_seq where aggregate_id='p-2'", Long.class))
                .isEqualTo(3L);
    }

    @Test
    void firstEventOfAnAggregateMustBeSeqOne() {
        processor.process(event(UuidV7.next(), "p-3", 2, 10, "PaymentCaptured"));
        assertThat(count("payflow.events.order.violations", "kind", "gap")).isEqualTo(1);
    }

    @Test
    void inOrderFiveEventPaymentsHaveNoViolations() {
        for (int p = 0; p < 200; p++) {
            for (long seq = 1; seq <= 5; seq++) {
                processor.process(event(UuidV7.next(), "q-" + p, seq, 1, "PaymentCaptured"));
            }
        }
        assertThat(reg.find("payflow.events.order.violations").counters()).isEmpty();
        assertThat(rows("effect_log")).isEqualTo(1_000);
    }

    @Test
    void unreadablePayloadIsNonRetryableAndLeavesNoTrace() {
        var bad = new EventMeta(UuidV7.next(), "PaymentAuthorized", 1, Instant.now(), null, "m-1", "p-9", "{not json", 0,
                "payment.events", 0);
        org.junit.jupiter.api.Assertions.assertThrows(NonRetryableEventException.class, () -> processor.process(bad));
        assertThat(rows("processed_events")).isZero();
        assertThat(rows("effect_log")).isZero();
    }

    /** The dedup insert and the effect share one transaction: a failing effect rolls the dedup row back too. */
    @Test
    void failureAfterDedupInsertRollsBackTheDedupRow() {
        jdbc.execute("ALTER TABLE effect_log ADD CONSTRAINT it_fail CHECK (event_id::text NOT LIKE 'ffffffff%')");
        try {
            var id = UUID.fromString("ffffffff-ffff-7fff-bfff-ffffffffffff");
            org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
                    () -> processor.process(event(id, "p-4", 1, 10, "PaymentAuthorized")));
            assertThat(rows("processed_events")).isZero();
            assertThat(rows("merchant_balance")).isZero();
            assertThat(rows("aggregate_seq")).isZero();
        } finally {
            jdbc.execute("ALTER TABLE effect_log DROP CONSTRAINT it_fail");
        }
    }
}
