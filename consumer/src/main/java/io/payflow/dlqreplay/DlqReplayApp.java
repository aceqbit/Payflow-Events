package io.payflow.dlqreplay;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import io.micrometer.core.instrument.MeterRegistry;
import io.payflow.common.EventsMetrics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * DLQ replay CLI. Reads payments.dlq with group dlq-replay, fixes each event and re-publishes it to payment.events with
 * its original key and headers, so the id is unchanged and the consumer's dedup applies it exactly once. One of only two
 * places allowed to produce (CLAUDE.md rule 1: relays and this CLI). Run it with the consumer jar:
 * <pre>java -cp consumer.jar -Dloader.main=io.payflow.dlqreplay.DlqReplayApp org.springframework.boot.loader.launch.PropertiesLauncher</pre>
 * Fix rules: drop the poison flags; a numeric-string amount becomes an integer; anything else is unfixable and skipped
 * (counted result=unfixable) so it stays visible in the DLQ topic but does not block the replay group.
 */
@SpringBootApplication
public class DlqReplayApp {
    private static final Logger log = LoggerFactory.getLogger(DlqReplayApp.class);
    private static final JsonMapper MAPPER = JsonMapper.builder().build();
    private static final Set<String> KEEP = Set.of("id", "type", "seq", "occurred_at", "traceparent", "merchant_id");

    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(DlqReplayApp.class, args)));
    }

    @Bean
    EventsMetrics eventsMetrics(MeterRegistry registry, @Value("${relay:poller}") String relay) {
        return new EventsMetrics(registry, relay);
    }

    @Bean
    CommandLineRunner replay(EventsMetrics metrics, @Value("${kafka.bootstrap}") String bootstrap,
                             @Value("${replay.dlq-topic:payments.dlq}") String dlq,
                             @Value("${replay.target-topic:payment.events}") String target,
                             @Value("${replay.group:dlq-replay}") String group,
                             @Value("${replay.idle-seconds:8}") int idleSeconds) {
        return args -> {
            Properties cp = new Properties();
            cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            cp.put(ConsumerConfig.GROUP_ID_CONFIG, group);
            cp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            cp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            Properties pp = new Properties();
            pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            pp.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
            pp.put(ProducerConfig.ACKS_CONFIG, "all");
            pp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            pp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            long replayed = 0;
            long unfixable = 0;
            long idleSince = System.currentTimeMillis();
            try (var consumer = new KafkaConsumer<String, String>(cp); var producer = new KafkaProducer<String, String>(pp)) {
                consumer.subscribe(List.of(dlq));
                while (System.currentTimeMillis() - idleSince < idleSeconds * 1000L) {
                    var records = consumer.poll(Duration.ofMillis(500));
                    if (records.isEmpty()) {
                        continue;
                    }
                    idleSince = System.currentTimeMillis();
                    for (ConsumerRecord<String, String> r : records) {
                        String fixed = fix(r.value());
                        if (fixed == null) {
                            unfixable++;
                            metrics.dlqReplayed("unfixable");
                            continue;
                        }
                        var out = new ProducerRecord<>(target, r.key(), fixed);
                        r.headers().forEach(h -> {
                            if (KEEP.contains(h.key())) {
                                out.headers().add(h.key(), h.value());
                            }
                        });
                        producer.send(out);
                        replayed++;
                        metrics.dlqReplayed("replayed");
                    }
                    producer.flush();        // published before the offsets move: a crash re-replays, dedup absorbs it
                    consumer.commitSync();
                }
            }
            log.info("DLQ_REPLAY_DONE replayed={} unfixable={}", replayed, unfixable);
            System.out.println("DLQ_REPLAY_DONE replayed=" + replayed + " unfixable=" + unfixable);
            Thread.sleep(6_000); // let the OTLP push carry payflow_events_dlq_replayed_total out
        };
    }

    /** The corrected payload, or null when the event cannot be repaired automatically. */
    static String fix(String payload) {
        try {
            JsonNode n = MAPPER.readTree(payload);
            if (!(n instanceof ObjectNode o)) {
                return null;
            }
            o.remove("poison");
            o.remove("fail_effect");
            JsonNode amount = o.get("amount_minor");
            if (amount == null) {
                return null;
            }
            if (!amount.isIntegralNumber()) {
                String text = amount.asString().trim();
                if (!text.matches("-?\\d+")) {
                    return null;
                }
                o.put("amount_minor", Long.parseLong(text));
            }
            return MAPPER.writeValueAsString(o);
        } catch (RuntimeException e) {
            return null;
        }
    }

    static String text(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }
}
