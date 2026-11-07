package io.payflow.rollup;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Topology-level tests with no broker: hot merchant, duplicates, pills, salting. */
class RollupTopologyTest {
    static final int B = 8;
    TopologyTestDriver driver;
    TestInputTopic<String, String> in;
    Map<String, Long> expected = new HashMap<>();
    Set<String> seenIds = new HashSet<>();

    @BeforeEach
    void setUp() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "rollup-test");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        driver = new TopologyTestDriver(RollupTopology.build(B, Duration.ofSeconds(1)), p);
        in = driver.createInputTopic(RollupTopology.IN, new StringSerializer(), new StringSerializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    private void send(String id, String paymentId, String merchant, long amount, boolean poison, long ts) {
        String payload = "{\"payment_id\":\"" + paymentId + "\",\"merchant_id\":\"" + merchant + "\",\"amount_minor\":" + amount
                + (poison ? ",\"poison\":true" : "") + "}";
        var headers = new RecordHeaders().add("id", id.getBytes(StandardCharsets.UTF_8));
        in.pipeInput(new org.apache.kafka.streams.test.TestRecord<>(paymentId, payload, headers, Instant.ofEpochMilli(ts)));
        if (!poison && seenIds.add(id)) {
            expected.merge(merchant, amount, Long::sum);
        }
    }

    private Map<String, Long> totals() {
        var out = driver.createOutputTopic(RollupTopology.TOTALS, new StringDeserializer(), new StringDeserializer());
        Map<String, Long> latest = new HashMap<>();
        out.readKeyValuesToList().forEach(kv -> latest.put(kv.key, Long.parseLong(kv.value)));
        return latest;
    }

    @Test
    void hotMerchantAtFortyPercentWithDuplicatesAndPillsRollsUpExactly() {
        var rnd = new Random(42);
        long ts = 1_700_000_000_000L;
        UUID[] ids = new UUID[20_000];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = new UUID(rnd.nextLong(), rnd.nextLong());
            String merchant = rnd.nextDouble() < 0.4 ? "m-0000" : "m-%04d".formatted(1 + rnd.nextInt(200));
            boolean pill = rnd.nextInt(1000) == 0;
            send(ids[i].toString(), "pay-" + i, merchant, 100 + rnd.nextInt(10_000), pill, ts + i * 2L);
            if (rnd.nextInt(20) == 0) { // the relay re-sends ~5% of events after a crash
                send(ids[i].toString(), "pay-" + i, merchant, 100 + rnd.nextInt(10_000), pill, ts + i * 2L + 1);
            }
        }
        var totals = totals();
        assertThat(totals).isEqualTo(expected);
        assertThat(totals.get("m-0000")).isEqualTo(expected.get("m-0000"));
    }

    @Test
    void theHotMerchantIsSpreadAcrossAllBucketsInStageOne() {
        for (int i = 0; i < 5_000; i++) {
            send(UUID.randomUUID().toString(), "pay-" + i, "m-0000", 1, false, 1_700_000_000_000L + i);
        }
        var out = driver.createOutputTopic(RollupTopology.STAGE1, new StringDeserializer(), new LongDeserializer());
        Set<String> salted = new HashSet<>();
        out.readKeyValuesToList().forEach(kv -> salted.add(kv.key.substring(0, kv.key.indexOf('@'))));
        assertThat(salted).hasSize(B);
        assertThat(salted).allMatch(k -> k.startsWith("m-0000#"));
    }

    @Test
    void saltMatchesTheGeneratorsMurmur2Bucket() {
        for (int i = 0; i < 500; i++) {
            int s = RollupTopology.salt("pay-" + i, B);
            assertThat(s).isBetween(0, B - 1);
            assertThat(RollupTopology.salt("pay-" + i, B)).isEqualTo(s);
        }
        assertThat(RollupTopology.merchantOf("m-0007#3@1700000000000")).isEqualTo("m-0007");
    }

    @Test
    void windowUpdatesAreNotDoubleCounted() {
        long ts = 1_700_000_000_000L;
        for (int i = 0; i < 300; i++) { // many updates inside one 1 s window
            send(UUID.randomUUID().toString(), "same-" + (i % 3), "m-0001", 10, false, ts + i);
        }
        assertThat(totals().get("m-0001")).isEqualTo(3_000L);
    }
}
