package io.payflow.common;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

/** The only place custom meters are created (plan section 5). OTel dots; Prometheus shows underscores + _seconds. */
public class EventsMetrics {
    private static final Duration[] E2E = ms(10, 25, 50, 100, 200, 500, 1000, 2000, 5000);
    private static final Duration[] STAGE = ms(5, 10, 25, 50, 100, 200, 500, 1000);

    private final MeterRegistry reg;
    private final String relay;
    private final Map<String, AtomicLong> lag = new ConcurrentHashMap<>();
    private final Timer dedupInsert;
    private final Timer gap;
    private final DistributionSummary dedupBatch;
    private final DistributionSummary relayBatch;
    private final Timer relayPoll;

    public EventsMetrics(MeterRegistry reg, String relay) {
        this.reg = reg;
        this.relay = relay;
        this.dedupInsert = Timer.builder("payflow.events.dedup.insert").serviceLevelObjectives(STAGE).register(reg);
        this.gap = Timer.builder("payflow.events.consumption.gap").serviceLevelObjectives(E2E).register(reg);
        this.dedupBatch = DistributionSummary.builder("payflow.events.dedup.batch.size").register(reg);
        this.relayBatch = DistributionSummary.builder("payflow.relay.batch.size").register(reg);
        this.relayPoll = Timer.builder("payflow.relay.poll").serviceLevelObjectives(STAGE).register(reg);
    }

    private static Duration[] ms(long... v) {
        Duration[] d = new Duration[v.length];
        for (int i = 0; i < v.length; i++) {
            d[i] = Duration.ofMillis(v[i]);
        }
        return d;
    }

    public void recordLatency(String topic, boolean poison, Duration relayLatency, Duration consumeLatency) {
        Timer.builder("payflow.events.e2e.latency").tags("topic", topic, "relay", relay, "poison", Boolean.toString(poison))
                .serviceLevelObjectives(E2E).register(reg).record(relayLatency.plus(consumeLatency));
        Timer.builder("payflow.events.relay.latency").tags("topic", topic, "relay", relay)
                .serviceLevelObjectives(STAGE).register(reg).record(relayLatency);
        Timer.builder("payflow.events.consume.latency").tags("topic", topic)
                .serviceLevelObjectives(STAGE).register(reg).record(consumeLatency);
    }

    public void consumerLag(String topic, int partition, double seconds) {
        lag.computeIfAbsent(topic + "/" + partition, k -> {
            AtomicLong v = new AtomicLong();
            Gauge.builder("payflow.events.consumer.lag", v, a -> a.get() / 1000.0).baseUnit("seconds")
                    .tags("topic", topic, "partition", Integer.toString(partition)).register(reg);
            return v;
        }).set((long) (seconds * 1000));
    }

    public void processed(String topic, String result, long n) {
        Counter.builder("payflow.events.processed").tags("topic", topic, "result", result).register(reg).increment(n);
    }

    public void orderViolation(String kind) {
        Counter.builder("payflow.events.order.violations").tags("kind", kind).register(reg).increment();
    }

    public void dedupBatchSize(int n) {
        dedupBatch.record(n);
    }

    public Timer dedupInsertTimer() {
        return dedupInsert;
    }

    public void retry(String stage) {
        Counter.builder("payflow.events.retry").tags("stage", stage).register(reg).increment();
    }

    public void dlqReplayed(String result) {
        Counter.builder("payflow.events.dlq.replayed").tags("result", result).register(reg).increment();
    }

    public void consumptionGap(Duration d) {
        gap.record(d);
    }

    public void relayBatch(int size, Duration poll) {
        relayBatch.record(size);
        relayPoll.record(poll);
    }

    public void relayPublished(long n) {
        Counter.builder("payflow.relay.published").register(reg).increment(n);
    }

    public void generatorSent(String strategy, long n) {
        Counter.builder("payflow.generator.sent").tags("strategy", strategy).register(reg).increment(n);
    }

    /** Hot-merchant share in basis points; exported as a 0..1 gauge. */
    public AtomicLong hotShare() {
        AtomicLong v = new AtomicLong();
        Gauge.builder("payflow.generator.hot.share", v, a -> a.get() / 10000.0).register(reg);
        return v;
    }

    private final Map<String, AtomicLong> verifierValues = new ConcurrentHashMap<>();

    /**
     * Verifier gauge under payflow.verifier.<name>. Setting an existing (name, labels) pair updates it, so the scheduled
     * verifier refreshes the same series on every pass.
     */
    public void verifierGauge(String name, double value, String runId, String scenario, String relay) {
        gauge("payflow.verifier." + name, value, runId, scenario, relay);
    }

    /** Gauge with an exact full name and the run labels (used for payflow.rollup.mismatch.minor). */
    public void gauge(String fullName, double value, String runId, String scenario, String relay) {
        String id = fullName + "|" + runId + "|" + scenario + "|" + relay;
        verifierValues.computeIfAbsent(id, k -> {
            AtomicLong v = new AtomicLong();
            Gauge.builder(fullName, v, a -> Double.longBitsToDouble(a.get()))
                    .tags(Tags.of("run_id", runId, "scenario", scenario, "relay", relay)).register(reg);
            return v;
        }).set(Double.doubleToLongBits(value));
    }
}
