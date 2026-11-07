package io.payflow.common;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

class TotalGateTest {
    @Test
    void budgetIsTotalMinusAlreadyCommitted() {
        var gate = new TotalGate(1_000, () -> 400);
        int granted = 0;
        for (int i = 0; i < 2_000; i++) {
            if (gate.claim()) {
                granted++;
            }
        }
        assertThat(granted).isEqualTo(600);
    }

    @Test
    void aReleasedClaimIsGrantedAgain() {
        var gate = new TotalGate(3, () -> 0);
        assertThat(gate.claim()).isTrue();
        assertThat(gate.claim()).isTrue();
        gate.release(); // that write did not commit
        assertThat(gate.claim()).isTrue();
        assertThat(gate.claim()).isTrue();
        assertThat(gate.claim()).isFalse();
    }

    @Test
    void restartAfterAKillContinuesWhereTheDatabaseSays() {
        var committed = new AtomicInteger(250_000);
        var restarted = new TotalGate(1_000_000, committed::get);
        long granted = 0;
        while (restarted.claim() && granted < 2_000_000) {
            granted++;
        }
        assertThat(granted).isEqualTo(750_000);
    }

    @Test
    void unlimitedWhenTotalIsZeroAndFinishFiresOnce() {
        var gate = new TotalGate(0, () -> 5);
        for (int i = 0; i < 10_000; i++) {
            assertThat(gate.claim()).isTrue();
        }
        assertThat(gate.firstToFinish()).isTrue();
        assertThat(gate.firstToFinish()).isFalse();
    }

    @Test
    void alreadyCompleteMeansNothingToWrite() {
        assertThat(new TotalGate(100, () -> 100).claim()).isFalse();
        assertThat(new TotalGate(100, () -> 150).claim()).isFalse();
    }
}
