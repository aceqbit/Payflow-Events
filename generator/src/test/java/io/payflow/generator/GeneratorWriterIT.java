package io.payflow.generator;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.tracing.Tracer;
import io.payflow.common.EventsMetrics;
import io.payflow.common.testing.TestInfra;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

class GeneratorWriterIT {
    static JdbcTemplate jdbc;
    static TransactionTemplate tx;

    @BeforeAll
    static void setUp() {
        Flyway.configure().dataSource(TestInfra.pgUrl(), TestInfra.pgUser(), TestInfra.pgPassword())
                .locations("classpath:db/migration").load().migrate();
        var ds = new DriverManagerDataSource(TestInfra.pgUrl(), TestInfra.pgUser(), TestInfra.pgPassword());
        jdbc = new JdbcTemplate(ds);
        tx = new TransactionTemplate(new DataSourceTransactionManager(ds));
    }

    @BeforeEach
    void clean() {
        jdbc.execute("TRUNCATE outbox, payments, pills");
    }

    private GeneratorWriter writer(KeyStrategy ks, double hot, double pill, String run, long seed) {
        return new GeneratorWriter(jdbc, tx, Tracer.NOOP, new EventsMetrics(new SimpleMeterRegistry(), "poller"), ks, 8,
                1000, hot, pill, run, seed);
    }

    private int count(String sql, Object... args) {
        return jdbc.queryForObject(sql, Integer.class, args);
    }

    @Test
    void sameSeedGivesTheSameEventsAndDifferentSeedDoesNot() {
        var a = writer(KeyStrategy.AGGREGATE, 0, 0, "seed-a", 7);
        for (int i = 0; i < 50; i++) {
            a.writeOne();
        }
        var b = writer(KeyStrategy.AGGREGATE, 0, 0, "seed-b", 7);
        for (int i = 0; i < 50; i++) {
            b.writeOne();
        }
        var c = writer(KeyStrategy.AGGREGATE, 0, 0, "seed-c", 8);
        for (int i = 0; i < 50; i++) {
            c.writeOne();
        }
        List<String> ra = jdbc.queryForList("select id from payments where run_id='seed-a' order by created_at, id", String.class);
        List<String> rb = jdbc.queryForList("select id from payments where run_id='seed-b' order by created_at, id", String.class);
        List<String> rc = jdbc.queryForList("select id from payments where run_id='seed-c' order by created_at, id", String.class);
        assertThat(ra).containsExactlyInAnyOrderElementsOf(rb);
        assertThat(ra).doesNotContainAnyElementsOf(rc);
    }

    @Test
    void hotMerchantGetsAboutFortyPercent() {
        var w = writer(KeyStrategy.AGGREGATE, 0.4, 0, "hot", 42);
        for (int i = 0; i < 5_000; i++) {
            w.writeOne();
        }
        double share = count("select count(*) from payments where run_id='hot' and merchant_id='m-0000'") / 5000.0;
        assertThat(share).isBetween(0.37, 0.43);
        assertThat(w.measuredHotShare()).isEqualTo(share);
    }

    @Test
    void uniformHasNoHotMerchant() {
        var w = writer(KeyStrategy.AGGREGATE, 0, 0, "uni", 42);
        for (int i = 0; i < 3_000; i++) {
            w.writeOne();
        }
        int max = count("select max(c) from (select count(*) c from payments where run_id='uni' group by merchant_id) t");
        assertThat(max).isLessThan(30); // expected 3 per merchant over 999 merchants
    }

    @Test
    void keyStrategyDecidesTheOutboxAggregateId() {
        var agg = writer(KeyStrategy.AGGREGATE, 0, 0, "k-agg", 1);
        var mer = writer(KeyStrategy.MERCHANT, 0, 0, "k-mer", 1);
        var sal = writer(KeyStrategy.SALTED, 0, 0, "k-sal", 1);
        for (int i = 0; i < 200; i++) {
            agg.writeOne();
            mer.writeOne();
            sal.writeOne();
        }
        assertThat(count("select count(*) from outbox o join payments p on p.event_id=o.id where p.run_id='k-agg' and o.aggregate_id = p.id")).isEqualTo(200);
        assertThat(count("select count(*) from outbox o join payments p on p.event_id=o.id where p.run_id='k-mer' and o.aggregate_id = p.merchant_id")).isEqualTo(200);
        assertThat(count("select count(*) from outbox o join payments p on p.event_id=o.id where p.run_id='k-sal' and o.aggregate_id ~ '^m-[0-9]{4}#[0-7]$'")).isEqualTo(200);
        assertThat(count("select count(distinct aggregate_id) from outbox o join payments p on p.event_id=o.id where p.run_id='k-sal'")).isGreaterThan(150);
    }

    @Test
    void saltIsStableAndWithinBuckets() {
        for (int i = 0; i < 1_000; i++) {
            int s = KeyStrategy.salt("pay-" + i, 8);
            assertThat(s).isBetween(0, 7);
            assertThat(KeyStrategy.salt("pay-" + i, 8)).isEqualTo(s);
        }
    }

    @Test
    void pillsAreRecordedInThePillsTableExactly() {
        var w = writer(KeyStrategy.AGGREGATE, 0, 0.01, "pill", 42);
        for (int i = 0; i < 5_000; i++) {
            w.writeOne();
        }
        assertThat(w.pillsInjected()).isBetween(20L, 90L);
        assertThat(count("select count(*) from pills where run_id='pill'")).isEqualTo((int) w.pillsInjected());
        assertThat(count("select count(*) from outbox o join pills pi on pi.event_id=o.id where (o.payload->>'poison')::boolean")).isEqualTo((int) w.pillsInjected());
        assertThat(count("select count(*) from outbox o join payments p on p.event_id=o.id where p.run_id='pill' and not (o.payload ? 'poison')")).isEqualTo(5_000 - (int) w.pillsInjected());
    }
}
