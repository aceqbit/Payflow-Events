package io.payflow.common;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

/** Rule 9: migration V1 must equal the Ledger contract. */
class OutboxContractTest {
    @Test
    void v1EqualsLedgerContract() throws Exception {
        Path contract = Path.of("..", "docs", "contract", "outbox.sql");
        Path v1 = Path.of("src", "main", "resources", "db", "migration", "V1__outbox.sql");
        assertThat(Files.readAllBytes(v1)).isEqualTo(Files.readAllBytes(contract));
    }
}
