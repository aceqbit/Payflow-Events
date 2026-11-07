package io.payflow.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import io.micrometer.core.instrument.MeterRegistry;
import io.payflow.common.OutboxRow;
import io.payflow.common.RecordShape;
import io.payflow.common.UuidV7;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import io.payflow.common.testing.TestInfra;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/** 1k payments x 5 events through real Kafka and Postgres while the consumer app is repeatedly stopped and restarted. */
class ConsumerRestartIT {

    static JdbcTemplate jdbc;
    static final String GROUP = java.util.UUID.randomUUID().toString();

    @BeforeAll
    static void setUp() throws Exception {
        Flyway.configure().dataSource(TestInfra.pgUrl(), TestInfra.pgUser(), TestInfra.pgPassword())
                .locations("classpath:db/migration").load().migrate();
        jdbc = new JdbcTemplate(new DriverManagerDataSource(TestInfra.pgUrl(), TestInfra.pgUser(), TestInfra.pgPassword()));
        jdbc.execute("TRUNCATE processed_events, effect_log, aggregate_seq, merchant_balance");
        try (var admin = AdminClient.create(java.util.Map.<String, Object>of("bootstrap.servers",
                TestInfra.kafka()))) {
            try {
                admin.createTopics(List.of(new NewTopic("itrestart.events", 12, (short) 1))).all().get();
            } catch (java.util.concurrent.ExecutionException e) {
                if (!(e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException)) {
                    throw e;
                }
            }
        }
    }

    private static ConfigurableApplicationContext startConsumer() {
        // command-line args, not builder properties: they must beat application.yml
        return new SpringApplicationBuilder(ConsumerApp.class).run(
                "--spring.datasource.url=" + TestInfra.pgUrl(),
                "--spring.datasource.username=" + TestInfra.pgUser(),
                "--spring.datasource.password=" + TestInfra.pgPassword(),
                "--spring.kafka.bootstrap-servers=" + TestInfra.kafka(),
                "--consumer.topic=itrestart.events",
                "--consumer.group=it-restart-" + GROUP,
                "--management.otlp.metrics.export.enabled=false",
                "--management.opentelemetry.tracing.export.otlp.endpoint=http://127.0.0.1:1/v1/traces",
                "--management.opentelemetry.logging.export.otlp.endpoint=http://127.0.0.1:1/v1/logs");
    }

    private static double violations(ConfigurableApplicationContext ctx) {
        return ctx.getBean(MeterRegistry.class).find("payflow.events.order.violations").counters().stream()
                .mapToDouble(c -> c.count()).sum();
    }

    @Test
    void thousandPaymentsFiveEventsWithRandomRestartsHaveZeroOrderViolations() throws Exception {
        int payments = 1_000;
        int perPayment = 5;
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TestInfra.kafka());
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (var producer = new KafkaProducer<String, String>(p)) {
            for (int seq = 1; seq <= perPayment; seq++) {
                for (int i = 0; i < payments; i++) {
                    String pid = "pay-" + i;
                    producer.send(RecordShape.toRecord(new OutboxRow(UuidV7.next(), "itrestart", pid,
                            "PaymentCaptured", "{\"payment_id\":\"" + pid + "\",\"amount_minor\":1}", seq,
                            Instant.now(), null, "m-" + (i % 50))));
                }
            }
            producer.flush();
        }

        // stop and restart the consumer app at random points while it is working through the backlog
        var rnd = new Random(42);
        double violations = 0;
        for (int restart = 0; restart < 5; restart++) {
            var ctx = startConsumer();
            Thread.sleep(500 + rnd.nextInt(2_000));
            violations += violations(ctx);
            ctx.close();
        }
        var last = startConsumer();
        try {
            await().atMost(Duration.ofSeconds(120)).until(() ->
                    jdbc.queryForObject("select count(*) from effect_log", Integer.class) >= payments * perPayment);
            await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(20)).until(() -> true);
            violations += violations(last);
        } finally {
            last.close();
        }

        assertThat(jdbc.queryForObject("select count(*) from effect_log", Integer.class)).isEqualTo(5_000);
        assertThat(jdbc.queryForObject("select count(distinct event_id) from effect_log", Integer.class))
                .isEqualTo(5_000);
        assertThat(jdbc.queryForObject("select count(*) from processed_events", Integer.class)).isEqualTo(5_000);
        assertThat(jdbc.queryForObject("select count(*) from aggregate_seq where last_seq = 5", Integer.class))
                .isEqualTo(1_000);
        assertThat(jdbc.queryForObject("select sum(balance_minor) from merchant_balance", Long.class)).isEqualTo(5_000L);
        assertThat(violations).isZero();
    }
}
