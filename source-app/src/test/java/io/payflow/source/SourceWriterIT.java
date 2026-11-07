package io.payflow.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.tracing.Tracer;
import io.payflow.common.EventsMetrics;
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
class SourceWriterIT {
    @Container
    static PostgreSQLContainer<?> pg = new PostgreSQLContainer<>("postgres:18");

    static JdbcTemplate jdbc;
    static TransactionTemplate tx;

    @BeforeAll
    static void migrate() {
        Flyway.configure().dataSource(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword())
                .locations("classpath:db/migration").load().migrate();
        var ds = new DriverManagerDataSource(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
        jdbc = new JdbcTemplate(ds);
        tx = new TransactionTemplate(new DataSourceTransactionManager(ds));
    }

    @BeforeEach
    void clean() {
        jdbc.execute("TRUNCATE outbox, payments");
    }

    private SourceWriter writer(OutboxMode mode, SimpleMeterRegistry reg) {
        return new SourceWriter(jdbc, tx, Tracer.NOOP, new EventsMetrics(reg, "poller"), mode, 10, "it-run");
    }

    @Test
    void retainWritesBusinessAndOutboxRowAtomicallyWithMatchingEventId() {
        var reg = new SimpleMeterRegistry();
        var w = writer(OutboxMode.RETAIN, reg);
        for (int i = 0; i < 20; i++) {
            w.writeOne();
        }
        assertThat(jdbc.queryForObject("select count(*) from outbox", Integer.class)).isEqualTo(20);
        assertThat(jdbc.queryForObject("select count(*) from payments", Integer.class)).isEqualTo(20);
        // anti-join both ways: every payment's event_id is an outbox id and vice versa
        assertThat(jdbc.queryForObject("""
                select count(*) from payments p left join outbox o on o.id = p.event_id where o.id is null""",
                Integer.class)).isZero();
        assertThat(jdbc.queryForObject("""
                select count(*) from outbox o left join payments p on p.event_id = o.id where p.id is null""",
                Integer.class)).isZero();
        Map<String, Object> row = jdbc.queryForMap("select * from outbox limit 1");
        assertThat(row.get("aggregate_type")).isEqualTo("payment");
        assertThat(row.get("aggregate_id")).isEqualTo(
                jdbc.queryForObject("select id from payments where event_id = ?", String.class, row.get("id")));
        assertThat(row.get("type")).isEqualTo("PaymentAuthorized");
        assertThat(row.get("seq")).isEqualTo(1L);
        assertThat(row.get("published_at")).isNull();
        assertThat((String) row.get("traceparent")).matches("00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]");
        assertThat(jdbc.queryForObject("select run_id from payments limit 1", String.class)).isEqualTo("it-run");
        assertThat(reg.get("payflow.generator.sent").tag("strategy", "uniform").counter().count()).isEqualTo(20.0);
    }

    @Test
    void deleteModeLeavesNoOutboxRowsButKeepsBusinessTruth() {
        var w = writer(OutboxMode.DELETE, new SimpleMeterRegistry());
        for (int i = 0; i < 10; i++) {
            w.writeOne();
        }
        assertThat(jdbc.queryForObject("select count(*) from outbox", Integer.class)).isZero();
        assertThat(jdbc.queryForObject("select count(*) from payments", Integer.class)).isEqualTo(10);
    }

    @Test
    void failureRollsBackBothRowsAndDoesNotCountAsSent() {
        var reg = new SimpleMeterRegistry();
        // a CHECK constraint on payments fails the second half of the single statement
        jdbc.execute("ALTER TABLE payments ADD CONSTRAINT it_fail CHECK (run_id <> 'boom')");
        try {
            var w = new SourceWriter(jdbc, tx, Tracer.NOOP, new EventsMetrics(reg, "poller"), OutboxMode.RETAIN, 10,
                    "boom");
            org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class, w::writeOne);
            assertThat(jdbc.queryForObject("select count(*) from outbox", Integer.class)).isZero();
            assertThat(jdbc.queryForObject("select count(*) from payments", Integer.class)).isZero();
            assertThat(reg.find("payflow.generator.sent").counter()).isNull();
        } finally {
            jdbc.execute("ALTER TABLE payments DROP CONSTRAINT it_fail");
        }
    }

    @Test
    void modeParsing() {
        assertThat(OutboxMode.parse("Retain")).isEqualTo(OutboxMode.RETAIN);
        assertThat(OutboxMode.parse("delete")).isEqualTo(OutboxMode.DELETE);
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> OutboxMode.parse("x"));
    }
}
