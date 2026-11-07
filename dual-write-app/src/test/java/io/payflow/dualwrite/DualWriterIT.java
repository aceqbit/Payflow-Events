package io.payflow.dualwrite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.tracing.Tracer;
import io.payflow.common.EventsMetrics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import io.payflow.common.testing.TestInfra;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

class DualWriterIT {

    static JdbcTemplate jdbc;
    static TransactionTemplate tx;
    static Producer<String, String> producer;

    @BeforeAll
    static void setUp() {
        Flyway.configure().dataSource(TestInfra.pgUrl(), TestInfra.pgUser(), TestInfra.pgPassword())
                .locations("classpath:db/migration").load().migrate();
        var ds = new DriverManagerDataSource(TestInfra.pgUrl(), TestInfra.pgUser(), TestInfra.pgPassword());
        jdbc = new JdbcTemplate(ds);
        tx = new TransactionTemplate(new DataSourceTransactionManager(ds));
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TestInfra.kafka());
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(p);
    }

    @AfterAll
    static void tearDown() {
        producer.close();
    }

    @BeforeEach
    void clean() {
        jdbc.execute("TRUNCATE payments");
    }

    /** Ids (header "id") of every record the writer handed to the producer in this test. */
    final Set<String> sentIds = ConcurrentHashMap.newKeySet();

    @SuppressWarnings("unchecked")
    private Producer<String, String> recording() {
        return (Producer<String, String>) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] {Producer.class}, (proxy, m, args) -> {
                    if (m.getName().equals("send")) {
                        var rec = (org.apache.kafka.clients.producer.ProducerRecord<String, String>) args[0];
                        sentIds.add(new String(rec.headers().lastHeader("id").value(), StandardCharsets.UTF_8));
                    }
                    try {
                        return m.invoke(producer, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private DualWriter writer(DualWriteMode mode, double rollbackRate) {
        sentIds.clear();
        return new DualWriter(jdbc, tx, Tracer.NOOP, new EventsMetrics(new SimpleMeterRegistry(), "dual-write"),
                recording(), mode, rollbackRate, 10, "it-run");
    }

    /** Reads payment.events from the start until every wanted id has been seen; returns those records by id. */
    private Map<String, ConsumerRecord<String, String>> awaitPublished(Set<String> wanted) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, TestInfra.kafka());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        var found = new ConcurrentHashMap<String, ConsumerRecord<String, String>>();
        try (var c = new KafkaConsumer<String, String>(p)) {
            c.subscribe(List.of("payment.events"));
            await().atMost(Duration.ofSeconds(30)).until(() -> {
                c.poll(Duration.ofMillis(200)).forEach(r -> {
                    String id = header(r, "id");
                    if (wanted.contains(id)) {
                        found.put(id, r);
                    }
                });
                return found.keySet().containsAll(wanted);
            });
        }
        return found;
    }

    private static String header(ConsumerRecord<String, String> r, String key) {
        var h = r.headers().lastHeader(key);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    @Test
    void dbFirstCommitsThenPublishesSameRecordShapeAsTheRelays() throws Exception {
        var w = writer(DualWriteMode.DB_FIRST, 0);
        for (int i = 0; i < 10; i++) {
            w.writeOne();
        }
        producer.flush();
        assertThat(jdbc.queryForObject("select count(*) from payments", Integer.class)).isEqualTo(10);
        assertThat(sentIds).hasSize(10);

        var published = awaitPublished(Set.copyOf(sentIds));
        assertThat(jdbc.queryForList("select event_id::text from payments", String.class))
                .containsExactlyInAnyOrderElementsOf(published.keySet());
        var r = published.values().iterator().next();
        assertThat(r.key()).isEqualTo(jdbc.queryForObject("select id from payments where event_id = ?::uuid",
                String.class, header(r, "id")));
        assertThat(header(r, "type")).isEqualTo("PaymentAuthorized");
        assertThat(header(r, "seq")).isEqualTo("1");
        assertThat(header(r, "occurred_at")).isNotNull();
        assertThat(header(r, "traceparent")).matches("00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]");
        assertThat(header(r, "merchant_id")).startsWith("m-");
        assertThat(r.value()).startsWith("{\"payment_id\":\"" + r.key() + "\"");
    }

    @Test
    void kafkaFirstForcedRollbackLeavesAPhantomInKafkaAndNothingInPostgres() throws Exception {
        var w = writer(DualWriteMode.KAFKA_FIRST, 1.0);
        for (int i = 0; i < 5; i++) {
            w.writeOne();
        }
        assertThat(sentIds).hasSize(5);
        assertThat(jdbc.queryForObject("select count(*) from payments", Integer.class)).isZero();
        // every event is in Kafka although no row committed: five phantoms
        assertThat(awaitPublished(Set.copyOf(sentIds))).hasSize(5);
    }

    @Test
    void kafkaFirstWithoutRollbackCommitsAndPublishes() throws Exception {
        var w = writer(DualWriteMode.KAFKA_FIRST, 0);
        for (int i = 0; i < 5; i++) {
            w.writeOne();
        }
        assertThat(jdbc.queryForObject("select count(*) from payments", Integer.class)).isEqualTo(5);
        var published = awaitPublished(Set.copyOf(sentIds));
        assertThat(jdbc.queryForList("select event_id::text from payments", String.class))
                .containsExactlyInAnyOrderElementsOf(published.keySet());
    }

    @Test
    void modeParsing() {
        assertThat(DualWriteMode.parse("db-first")).isEqualTo(DualWriteMode.DB_FIRST);
        assertThat(DualWriteMode.parse("kafka-first")).isEqualTo(DualWriteMode.KAFKA_FIRST);
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> DualWriteMode.parse("x"));
    }
}
