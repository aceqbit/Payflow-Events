package io.payflow.verifier;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import io.payflow.common.testing.TestInfra;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/** Seeded fixtures: each failure kind is detected exactly, in its own run id, and a clean run is all zeros. */
class VerificationIT {
    static JdbcTemplate jdbc;
    static Verification verification;

    @BeforeAll
    static void setUp() {
        Flyway.configure().dataSource(TestInfra.pgUrl(), TestInfra.pgUser(), TestInfra.pgPassword())
                .locations("classpath:db/migration").load().migrate();
        jdbc = new JdbcTemplate(new DriverManagerDataSource(TestInfra.pgUrl(), TestInfra.pgUser(), TestInfra.pgPassword()));
        verification = new Verification(jdbc);
    }

    private static String run() {
        return "it-" + UUID.randomUUID();
    }

    /** Commits n payments for the run (merchant m-1, amount 100 each) and returns their event ids. */
    private List<UUID> committed(String run, int n, String merchant, long amount) {
        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            ids.add(jdbc.queryForObject("""
                    insert into payments (id, merchant_id, amount_minor, version, event_id, run_id)
                    values (?, ?, ?, 1, uuidv7(), ?) returning event_id""", UUID.class,
                    run + "-" + UUID.randomUUID(), merchant, amount, run));
        }
        return ids;
    }

    private void kafka(String run, UUID id, String key, int part, long off, long seq) {
        jdbc.update("insert into verifier_kafka (run_id, topic, part, off, event_id, agg_key, seq) values (?, 'payment.events', ?, ?, ?, ?, ?)",
                run, part, off, id, key, seq);
    }

    private void processed(UUID id, int effects) {
        jdbc.update("insert into processed_events (event_id) values (?) on conflict do nothing", id);
        for (int i = 0; i < effects; i++) {
            jdbc.update("insert into effect_log (event_id) values (?)", id);
        }
    }

    /** A fully delivered, fully processed run. */
    private String cleanRun(int n) {
        String run = run();
        var ids = committed(run, n, "m-1", 100);
        for (int i = 0; i < ids.size(); i++) {
            kafka(run, ids.get(i), "k-" + i, i % 3, i, 1);
            processed(ids.get(i), 1);
        }
        return run;
    }

    @Test
    void cleanRunIsAllZeros() {
        String run = cleanRun(200);
        var r = verification.verify(run, false);
        assertThat(r).isEqualTo(new Verification.Result(200, 0, 0, 0, 0, 0, 0, 0, 0));
        assertThat(r.outboxClean()).isTrue();
    }

    @Test
    void lostEventsAreCommittedButNeverProcessed() {
        String run = cleanRun(50);
        committed(run, 5, "m-1", 100); // committed, never published, never processed
        var r = verification.verify(run, false);
        assertThat(r.lost()).isEqualTo(5);
        assertThat(r.phantom()).isZero();
        assertThat(r.duplicateEffects()).isZero();
        assertThat(r.outboxClean()).isFalse();
    }

    @Test
    void deadLetteredEventsAreAccountedForNotLost() {
        String run = cleanRun(20);
        var dead = committed(run, 3, "m-1", 100); // committed, never processed, but sitting in the DLQ
        dead.forEach(id -> jdbc.update("insert into verifier_dlq (run_id, event_id) values (?, ?)", run, id));
        var r = verification.verify(run, false);
        assertThat(r.lost()).isZero();
        assertThat(r.dlqCount()).isEqualTo(3);
        committed(run, 2, "m-1", 100); // two more with no DLQ entry are lost
        assertThat(verification.verify(run, false).lost()).isEqualTo(2);
    }

    @Test
    void phantomEventsArePublishedButNeverCommitted() {
        String run = cleanRun(50);
        for (int i = 0; i < 3; i++) {
            kafka(run, UUID.randomUUID(), "ghost-" + i, 0, 1_000 + i, 1);
        }
        var r = verification.verify(run, false);
        assertThat(r.phantom()).isEqualTo(3);
        assertThat(r.lost()).isZero();
    }

    @Test
    void duplicateEffectsCountExtraEffectsPerEventId() {
        String run = cleanRun(20);
        UUID victim = jdbc.queryForObject("select event_id from payments where run_id = ? limit 1", UUID.class, run);
        jdbc.update("insert into effect_log (event_id) values (?)", victim);
        jdbc.update("insert into effect_log (event_id) values (?)", victim); // three effects in total: 2 extra
        var r = verification.verify(run, false);
        assertThat(r.duplicateEffects()).isEqualTo(2);
        assertThat(r.lost()).isZero();
    }

    @Test
    void duplicatesReceivedCountRedeliveriesAndAreNotFailures() {
        String run = cleanRun(10);
        UUID id = jdbc.queryForObject("select event_id from payments where run_id = ? limit 1", UUID.class, run);
        String key = jdbc.queryForObject("select id from payments where event_id = ?", String.class, id);
        for (int i = 0; i < 3; i++) {
            kafka(run, id, key, 0, 5_000 + i, 1); // same id, same seq, redelivered three more times
        }
        var r = verification.verify(run, false);
        assertThat(r.duplicatesReceived()).isEqualTo(3);
        assertThat(r.orderViolations()).isZero();
        assertThat(r.outboxClean()).isTrue();
    }

    @Test
    void orderViolationsAreReversalsWithinAKey() {
        String run = run();
        var ids = committed(run, 4, "m-1", 100);
        // key A: seq 1, 2, then 1 again as a different event (reversal); key B: strictly ordered
        kafka(run, ids.get(0), "A", 0, 0, 1);
        kafka(run, ids.get(1), "A", 0, 1, 2);
        kafka(run, ids.get(2), "A", 0, 2, 1);
        kafka(run, ids.get(3), "B", 1, 0, 1);
        ids.forEach(i -> processed(i, 1));
        assertThat(verification.verify(run, false).orderViolations()).isEqualTo(1);
    }

    @Test
    void dlqCountIsDistinctEventIdsAndPillsAreCounted() {
        String run = cleanRun(10);
        UUID a = UUID.randomUUID();
        UUID b = UUID.randomUUID();
        jdbc.update("insert into verifier_dlq (run_id, event_id) values (?, ?), (?, ?), (?, ?)", run, a, run, a, run, b);
        for (int i = 0; i < 4; i++) {
            jdbc.update("insert into pills (run_id, event_id) values (?, ?)", run, UUID.randomUUID());
        }
        var r = verification.verify(run, false);
        assertThat(r.dlqCount()).isEqualTo(2);
        assertThat(r.pillsInjected()).isEqualTo(4);
    }

    @Test
    void rollupMismatchSumsAbsoluteDifferencePerMerchant() {
        String run = run();
        committed(run, 10, "m-1", 100);   // source total 1000
        committed(run, 1, "m-2", 50);     // source total 50, missing from the totals topic
        jdbc.update("insert into verifier_totals (run_id, merchant_id, total_minor) values (?, 'm-1', 993)", run); // off by 7
        assertThat(verification.verify(run, true).rollupMismatchMinor()).isEqualTo(57);
        assertThat(verification.verify(run, false).rollupMismatchMinor()).isZero(); // rollup disabled for this run
    }

    @Test
    void runsAreIsolatedByRunId() {
        String a = cleanRun(30);
        String b = run();
        committed(b, 7, "m-1", 100);
        assertThat(verification.verify(a, false).lost()).isZero();
        assertThat(verification.verify(b, false).lost()).isEqualTo(7);
    }
}
