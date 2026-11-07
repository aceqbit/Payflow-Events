package io.payflow.common;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.DriverManager;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class MigrationIT {
    @Container
    static PostgreSQLContainer<?> pg = new PostgreSQLContainer<>("postgres:18");

    @Test
    void migratesOnPostgres18() throws Exception {
        Flyway.configure().dataSource(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword())
                .locations("classpath:db/migration").load().migrate();
        try (var c = DriverManager.getConnection(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
             var st = c.createStatement()) {
            st.execute("insert into outbox(aggregate_type,aggregate_id,type,payload,seq,merchant_id) values ('payment','p1','T','{}',1,'m1')");
            st.execute("insert into processed_events(event_id) values (gen_random_uuid())");
            var rs = st.executeQuery("select count(*) from pg_inherits where inhparent='processed_events'::regclass");
            rs.next();
            assertThat(rs.getInt(1)).isGreaterThanOrEqualTo(8);
        }
    }
}
