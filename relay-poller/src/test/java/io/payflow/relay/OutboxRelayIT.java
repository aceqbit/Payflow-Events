package io.payflow.relay;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.payflow.common.EventsMetrics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
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
import org.testcontainers.kafka.KafkaContainer;

@Testcontainers
class OutboxRelayIT {
    @Container
    static PostgreSQLContainer<?> pg = new PostgreSQLContainer<>("postgres:18");
    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:4.1.0");

    static JdbcTemplate jdbc;
    static TransactionTemplate tx;
    static Producer<String, String> producer;

    @BeforeAll
    static void setUp() {
        Flyway.configure().dataSource(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword())
                .locations("classpath:db/migration").load().migrate();
        var ds = new DriverManagerDataSource(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
        jdbc = new JdbcTemplate(ds);
        tx = new TransactionTemplate(new DataSourceTransactionManager(ds));
        producer = newProducer(kafka.getBootstrapServers());
    }

    @AfterAll
    static void tearDown() {
        producer.close();
    }

    @BeforeEach
    void clean() {
        jdbc.execute("TRUNCATE outbox");
    }

    static Producer<String, String> newProducer(String bootstrap) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(p);
    }

    private OutboxRelay relay(Producer<String, String> prod, int batch) {
        return new OutboxRelay(jdbc, tx, prod, new EventsMetrics(new SimpleMeterRegistry(), "poller"), batch);
    }

    private static String header(ConsumerRecord<String, String> r, String key) {
        var h = r.headers().lastHeader(key);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    /** Consumes payment.events from the start until every wanted id has been seen. Returns all records seen. */
    private List<ConsumerRecord<String, String>> consumeUntil(Set<String> wantedIds) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        var all = new java.util.ArrayList<ConsumerRecord<String, String>>();
        var seen = ConcurrentHashMap.<String>newKeySet();
        try (var c = new KafkaConsumer<String, String>(p)) {
            c.subscribe(List.of("payment.events"));
            await().atMost(Duration.ofSeconds(60)).until(() -> {
                c.poll(Duration.ofMillis(200)).forEach(r -> {
                    all.add(r);
                    seen.add(header(r, "id"));
                });
                return seen.containsAll(wantedIds);
            });
            // linger briefly on a quiet topic to catch any extra copies of already-seen ids
            await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(10)).until(() -> {
                c.poll(Duration.ofMillis(200)).forEach(all::add);
                return true;
            });
        }
        return all;
    }

    /** Contract (plan section 3): key = aggregate_id, value = payload compact, six headers with exact values. */
    @Test
    void oneOutboxRowBecomesTheExactRecord() {
        UUID id = jdbc.queryForObject("""
                insert into outbox (aggregate_type, aggregate_id, type, payload, seq, occurred_at, traceparent, merchant_id)
                values ('payment', 'pay-123', 'PaymentCaptured',
                        '{"payment_id": "pay-123", "merchant_id": "m-7", "amount_minor": 4200, "currency": "USD"}',
                        3, '2026-11-07T10:15:30.123456Z',
                        '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01', 'm-7')
                returning id""", UUID.class);

        assertThat(relay(producer, 500).pollOnce()).isEqualTo(1);

        var recs = consumeUntil(Set.of(id.toString()));
        assertThat(recs).hasSize(1);
        var r = recs.get(0);
        assertThat(r.topic()).isEqualTo("payment.events");
        assertThat(r.key()).isEqualTo("pay-123");
        assertThat(r.value().getBytes(StandardCharsets.UTF_8)).isEqualTo(
                "{\"payment_id\":\"pay-123\",\"merchant_id\":\"m-7\",\"amount_minor\":4200,\"currency\":\"USD\"}"
                        .getBytes(StandardCharsets.UTF_8));
        assertThat(header(r, "id")).isEqualTo(id.toString());
        assertThat(header(r, "type")).isEqualTo("PaymentCaptured");
        assertThat(header(r, "seq")).isEqualTo("3");
        assertThat(header(r, "occurred_at")).isEqualTo("2026-11-07T10:15:30.123456Z");
        assertThat(header(r, "traceparent")).isEqualTo("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
        assertThat(header(r, "merchant_id")).isEqualTo("m-7");
        assertThat(r.headers().toArray()).hasSize(6);
        assertThat(jdbc.queryForObject("select count(*) from outbox where published_at is not null", Integer.class))
                .isEqualTo(1);
    }

    /** Two relay instances against one outbox: SKIP LOCKED means no row is published twice (no crashes injected). */
    @Test
    void twoInstancesNeverDoublePick() {
        int rows = 5_000;
        jdbc.update("""
                insert into outbox (aggregate_type, aggregate_id, type, payload, seq, merchant_id)
                select 'payment', 'p-' || g, 'PaymentAuthorized',
                       jsonb_build_object('payment_id', 'p-' || g, 'merchant_id', 'm-1', 'amount_minor', g),
                       1, 'm-1'
                from generate_series(1, ?) g""", rows);
        Set<String> ids = Set.copyOf(jdbc.queryForList("select id::text from outbox", String.class));
        assertThat(ids).hasSize(rows);

        var stop = new AtomicBoolean();
        var polls = new AtomicInteger();
        var producerB = newProducer(kafka.getBootstrapServers());
        try {
            var a = relay(producer, 200);
            var b = relay(producerB, 200);
            Runnable runA = () -> drain(a, stop, polls);
            Runnable runB = () -> drain(b, stop, polls);
            Thread.ofVirtual().start(runA);
            Thread.ofVirtual().start(runB);
            await().atMost(Duration.ofSeconds(60)).until(() ->
                    jdbc.queryForObject("select count(*) from outbox where published_at is null", Integer.class) == 0);
            stop.set(true);
        } finally {
            stop.set(true);
            producerB.close();
        }

        var recs = consumeUntil(ids);
        Map<String, Long> perId = recs.stream().collect(java.util.stream.Collectors.groupingBy(
                r -> header(r, "id"), java.util.stream.Collectors.counting()));
        assertThat(perId.keySet()).isEqualTo(ids);
        assertThat(perId.values()).allMatch(n -> n == 1L);
    }

    private static void drain(OutboxRelay relay, AtomicBoolean stop, AtomicInteger polls) {
        while (!stop.get()) {
            polls.incrementAndGet();
            if (relay.pollOnce() == 0) {
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
    }

    /** A batch whose send is not acked stays unpublished (rolled back), and is published by the next healthy poll. */
    @Test
    void failedSendRollsTheBatchBackAndItIsRetried() {
        for (int i = 0; i < 3; i++) {
            jdbc.update("""
                    insert into outbox (aggregate_type, aggregate_id, type, payload, seq, merchant_id)
                    values ('payment', ?, 'PaymentAuthorized', '{"payment_id":"x"}', 1, 'm-1')""", "f-" + i);
        }
        // a closed producer fails every send, standing in for a broker that never acks
        var broken = newProducer(kafka.getBootstrapServers());
        broken.close();
        org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class, () -> relay(broken, 500).pollOnce());
        assertThat(jdbc.queryForObject("select count(*) from outbox where published_at is null", Integer.class))
                .isEqualTo(3);

        assertThat(relay(producer, 500).pollOnce()).isEqualTo(3);
        assertThat(jdbc.queryForObject("select count(*) from outbox where published_at is null", Integer.class))
                .isZero();
    }
}
