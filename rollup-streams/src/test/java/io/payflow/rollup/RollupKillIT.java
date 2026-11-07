package io.payflow.rollup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import io.payflow.common.testing.TestInfra;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.Test;

/** Hot merchant at 40%, a Streams instance killed and replaced mid-run: totals converge to exactly the source sums. */
class RollupKillIT {
    private KafkaStreams instance(String appId, String in, String stage1, String totals, Path stateDir) {
        Properties p = RollupApp.streamsProps(TestInfra.kafka(), appId, 2, 1, stateDir.toString());
        var streams = new KafkaStreams(RollupTopology.build(in, stage1, totals, 8, Duration.ofSeconds(1), Duration.ofHours(1)), p);
        streams.start();
        return streams;
    }

    @Test
    void totalsAreExactAfterAnInstanceKill() throws Exception {
        String id = UUID.randomUUID().toString().substring(0, 8);
        String in = "rk." + id + ".events";
        String stage1 = "rk." + id + ".stage1";
        String totals = "rk." + id + ".totals";
        String appId = "rollup-it-" + id;
        try (var admin = AdminClient.create(Map.<String, Object>of("bootstrap.servers", TestInfra.kafka()))) {
            admin.createTopics(List.of(new NewTopic(in, 6, (short) 1), new NewTopic(stage1, 6, (short) 1),
                    new NewTopic(totals, 3, (short) 1))).all().get();
        }

        var rnd = new Random(7);
        Map<String, Long> expected = new HashMap<>();
        Set<String> ids = new HashSet<>();
        Properties pp = new Properties();
        pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TestInfra.kafka());
        pp.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        pp.put(ProducerConfig.ACKS_CONFIG, "all");
        pp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Path d1 = Files.createTempDirectory("rollup1");
        Path d2 = Files.createTempDirectory("rollup2");
        Path d3 = Files.createTempDirectory("rollup3");
        var a = instance(appId, in, stage1, totals, d1);
        var b = instance(appId, in, stage1, totals, d2);
        KafkaStreams c = null;
        try (var producer = new KafkaProducer<String, String>(pp)) {
            for (int i = 0; i < 20_000; i++) {
                String eventId = new UUID(rnd.nextLong(), rnd.nextLong()).toString();
                String merchant = rnd.nextDouble() < 0.4 ? "m-0000" : "m-%04d".formatted(1 + rnd.nextInt(100));
                long amount = 100 + rnd.nextInt(10_000);
                String payload = "{\"payment_id\":\"pay-" + i + "\",\"merchant_id\":\"" + merchant + "\",\"amount_minor\":" + amount + "}";
                int copies = rnd.nextInt(25) == 0 ? 2 : 1; // duplicate delivery of ~4% of events
                for (int k = 0; k < copies; k++) {
                    var rec = new ProducerRecord<>(in, "pay-" + i, payload);
                    rec.headers().add("id", eventId.getBytes());
                    producer.send(rec);
                }
                if (ids.add(eventId)) {
                    expected.merge(merchant, amount, Long::sum);
                }
                if (i == 8_000) {
                    a.close(Duration.ZERO); // instance killed mid-run, without waiting for a clean shutdown
                    c = instance(appId, in, stage1, totals, d3);
                }
            }
            producer.flush();

            Properties cp = new Properties();
            cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, TestInfra.kafka());
            cp.put(ConsumerConfig.GROUP_ID_CONFIG, "rk-check-" + id);
            cp.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
            cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            Map<String, Long> latest = new HashMap<>();
            try (var consumer = new KafkaConsumer<String, String>(cp)) {
                var tps = consumer.partitionsFor(totals).stream().map(i -> new TopicPartition(totals, i.partition())).toList();
                consumer.assign(tps);
                consumer.seekToBeginning(tps);
                await().atMost(Duration.ofSeconds(120)).until(() -> {
                    consumer.poll(Duration.ofMillis(300)).forEach(r -> latest.put(r.key(), Long.parseLong(r.value())));
                    return latest.equals(expected);
                });
            }
            assertThat(latest).isEqualTo(expected);
            assertThat(latest.get("m-0000")).isEqualTo(expected.get("m-0000"));
        } finally {
            b.close(Duration.ofSeconds(10));
            if (c != null) {
                c.close(Duration.ofSeconds(10));
            }
        }
    }
}
