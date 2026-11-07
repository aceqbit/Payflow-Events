package io.payflow.verifier;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.jdbc.core.JdbcTemplate;

/** Streams a topic into the verifier staging tables in batches, bounded by the end offsets at scan start. */
public class KafkaScanner {
    private static final int BATCH = 5_000;

    private final JdbcTemplate jdbc;
    private final String bootstrap;

    public KafkaScanner(JdbcTemplate jdbc, String bootstrap) {
        this.jdbc = jdbc;
        this.bootstrap = bootstrap;
    }

    private KafkaConsumer<String, String> consumer() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "verifier-" + UUID.randomUUID());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        p.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(p);
    }

    static String header(ConsumerRecord<?, ?> r, String key) {
        var h = r.headers().lastHeader(key);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    /** Reads the topic from startOffsets (partition -> offset, missing = earliest) up to the end offsets at call time. */
    private void scan(String topic, Map<Integer, Long> startOffsets, Consumer<List<ConsumerRecord<String, String>>> sink) {
        try (var c = consumer()) {
            var infos = c.partitionsFor(topic, Duration.ofSeconds(10));
            if (infos == null || infos.isEmpty()) {
                return;
            }
            List<TopicPartition> tps = infos.stream().map(i -> new TopicPartition(topic, i.partition())).toList();
            c.assign(tps);
            var end = c.endOffsets(tps);
            var begin = c.beginningOffsets(tps);
            for (var tp : tps) {
                c.seek(tp, Math.max(startOffsets.getOrDefault(tp.partition(), 0L), begin.get(tp)));
            }
            List<ConsumerRecord<String, String>> buf = new ArrayList<>(BATCH);
            while (tps.stream().anyMatch(tp -> c.position(tp) < end.get(tp))) {
                for (var rec : c.poll(Duration.ofMillis(500))) {
                    var tp = new TopicPartition(rec.topic(), rec.partition());
                    if (rec.offset() < end.get(tp)) {
                        buf.add(rec);
                    }
                    if (buf.size() >= BATCH) {
                        sink.accept(buf);
                        buf = new ArrayList<>(BATCH);
                    }
                }
            }
            if (!buf.isEmpty()) {
                sink.accept(buf);
            }
        }
    }

    public long scanEvents(String runId, String topic, Map<Integer, Long> startOffsets) {
        jdbc.update("delete from verifier_kafka where run_id = ? and topic = ?", runId, topic);
        long[] n = {0};
        scan(topic, startOffsets, recs -> {
            jdbc.batchUpdate("""
                    insert into verifier_kafka (run_id, topic, part, off, event_id, agg_key, seq)
                    values (?, ?, ?, ?, ?::uuid, ?, ?) on conflict do nothing""", recs, recs.size(), (ps, r) -> {
                ps.setString(1, runId);
                ps.setString(2, r.topic());
                ps.setInt(3, r.partition());
                ps.setLong(4, r.offset());
                ps.setString(5, header(r, "id"));
                ps.setString(6, r.key());
                ps.setLong(7, Long.parseLong(header(r, "seq")));
            });
            n[0] += recs.size();
        });
        return n[0];
    }

    /** Ids of dead-lettered events (the DLQ is small, so it is read in full). Dead-letter records keep the id header. */
    public long scanDlq(String runId, String dlqTopic) {
        jdbc.update("delete from verifier_dlq where run_id = ?", runId);
        long[] n = {0};
        scan(dlqTopic, Map.of(), recs -> {
            var withId = recs.stream().filter(r -> header(r, "id") != null).toList();
            jdbc.batchUpdate("insert into verifier_dlq (run_id, event_id) values (?, ?::uuid)", withId, withId.size(),
                    (ps, r) -> {
                        ps.setString(1, runId);
                        ps.setString(2, header(r, "id"));
                    });
            n[0] += withId.size();
        });
        return n[0];
    }

    /** Latest value per merchant of the compacted totals topic (value = decimal minor units). */
    public void scanTotals(String runId, String totalsTopic) {
        jdbc.update("delete from verifier_totals where run_id = ?", runId);
        Map<String, Long> latest = new HashMap<>();
        scan(totalsTopic, Map.of(), recs -> recs.forEach(r -> {
            if (r.key() != null && r.value() != null) {
                latest.put(r.key(), Long.parseLong(r.value().trim()));
            }
        }));
        List<Map.Entry<String, Long>> rows = new ArrayList<>(latest.entrySet());
        jdbc.batchUpdate("insert into verifier_totals (run_id, merchant_id, total_minor) values (?, ?, ?)", rows,
                rows.size(), (ps, e) -> {
                    ps.setString(1, runId);
                    ps.setString(2, e.getKey());
                    ps.setLong(3, e.getValue());
                });
    }
}
