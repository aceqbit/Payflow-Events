package io.payflow.common.testing;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Integration-test infrastructure. Default: real Postgres 18 and Kafka 4.1 via Testcontainers (CLAUDE.md testing rules).
 * Machines without Docker can point the same tests at real external instances with PAYFLOW_TEST_PG_URL (plus
 * PAYFLOW_TEST_PG_USER / PAYFLOW_TEST_PG_PASSWORD) and PAYFLOW_TEST_KAFKA. Still real Kafka and Postgres, never embedded.
 * Testcontainers is an optional dependency of common, so this class is only used from tests.
 */
public final class TestInfra {
    private static PostgreSQLContainer<?> pg;
    private static KafkaContainer kafka;

    private TestInfra() {
    }

    private static String env(String k, String def) {
        String v = System.getenv(k);
        return v == null || v.isBlank() ? def : v;
    }

    private static synchronized void startPg() {
        if (pg == null) {
            pg = new PostgreSQLContainer<>("postgres:18");
            pg.start();
        }
    }

    private static synchronized void startKafka() {
        if (kafka == null) {
            kafka = new KafkaContainer("apache/kafka:4.1.0");
            kafka.start();
        }
    }

    public static String pgUrl() {
        String ext = env("PAYFLOW_TEST_PG_URL", null);
        if (ext != null) {
            return ext;
        }
        startPg();
        return pg.getJdbcUrl();
    }

    public static String pgUser() {
        return env("PAYFLOW_TEST_PG_URL", null) != null ? env("PAYFLOW_TEST_PG_USER", "payflow") : pgContainerUser();
    }

    public static String pgPassword() {
        return env("PAYFLOW_TEST_PG_URL", null) != null ? env("PAYFLOW_TEST_PG_PASSWORD", "payflow") : pgContainerPassword();
    }

    private static String pgContainerUser() {
        startPg();
        return pg.getUsername();
    }

    private static String pgContainerPassword() {
        startPg();
        return pg.getPassword();
    }

    public static String kafka() {
        String ext = env("PAYFLOW_TEST_KAFKA", null);
        if (ext != null) {
            return ext;
        }
        startKafka();
        return kafka.getBootstrapServers();
    }
}
