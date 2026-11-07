package io.payflow.verifier;

import org.springframework.jdbc.core.JdbcTemplate;

/**
 * The single oracle for correctness (CLAUDE.md). Every figure is one SQL aggregate over indexed tables; no id set is
 * ever loaded into memory. Inputs: payments (committed truth, run_id), verifier_kafka (staged scan of the event topic),
 * processed_events / effect_log (consumer side), verifier_dlq, verifier_totals and pills.
 */
public class Verification {
    public record Result(long committed, long lost, long phantom, long duplicateEffects, long duplicatesReceived,
                         long orderViolations, long dlqCount, long pillsInjected, long rollupMismatchMinor) {
        public boolean outboxClean() {
            return lost == 0 && phantom == 0 && duplicateEffects == 0 && orderViolations == 0 && rollupMismatchMinor == 0;
        }
    }

    private final JdbcTemplate jdbc;

    public Verification(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private long one(String sql, Object... args) {
        Long v = jdbc.queryForObject(sql, Long.class, args);
        return v == null ? 0 : v;
    }

    public Result verify(String runId, boolean checkRollup) {
        long committed = one("select count(*) from payments where run_id = ?", runId);

        // committed but neither processed nor dead-lettered: lost end to end (dual-write db-first shows up here)
        long lost = one("""
                select count(*) from payments p
                where p.run_id = ?
                  and not exists (select 1 from processed_events e where e.event_id = p.event_id)
                  and not exists (select 1 from verifier_dlq d where d.run_id = p.run_id and d.event_id = p.event_id)""", runId);

        // published but never committed: phantom (dual-write kafka-first shows up here)
        long phantom = one("""
                select count(distinct k.event_id) from verifier_kafka k
                where k.run_id = ?
                  and not exists (select 1 from payments p where p.event_id = k.event_id and p.run_id = ?)""",
                runId, runId);

        // more than one effect for one event id
        long duplicateEffects = one("""
                select coalesce(sum(c - 1), 0) from (
                  select count(*) c from effect_log e
                  where e.event_id in (select event_id from payments where run_id = ?)
                     or e.event_id in (select event_id from verifier_kafka where run_id = ?)
                  group by e.event_id having count(*) > 1) d""", runId, runId);

        long duplicatesReceived = one("""
                select count(*) - count(distinct event_id) from verifier_kafka where run_id = ?""", runId);

        // a record whose seq is lower than one already seen for the same key (equal seq is a redelivery, not a reversal)
        long orderViolations = one("""
                select count(*) from (
                  select seq, max(seq) over (partition by agg_key order by part, off
                                             rows between unbounded preceding and 1 preceding) m
                  from verifier_kafka where run_id = ?) t
                where seq < m""", runId);

        long dlq = one("select count(distinct event_id) from verifier_dlq where run_id = ?", runId);
        long pills = one("select count(*) from pills where run_id = ?", runId);

        long rollup = checkRollup ? one("""
                select coalesce(sum(abs(s.total - coalesce(t.total_minor, 0))), 0) from (
                  select merchant_id, sum(amount_minor) total from payments p where run_id = ?
                    and not exists (select 1 from pills pi where pi.event_id = p.event_id) group by merchant_id) s
                left join verifier_totals t on t.run_id = ? and t.merchant_id = s.merchant_id""", runId, runId) : 0;

        return new Result(committed, lost, phantom, duplicateEffects, duplicatesReceived, orderViolations, dlq, pills,
                rollup);
    }
}
