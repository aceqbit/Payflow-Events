package io.payflow.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

class PacedLoadTest {
    @Test
    void neverRunsAheadOfScheduleAndKeepsUp() {
        int rate = 1_000;
        int workers = 8;
        var count = new AtomicLong();
        long t0 = System.nanoTime();
        try (var load = new PacedLoad(rate, workers, count::incrementAndGet, t -> {
            throw new AssertionError(t);
        })) {
            await().atMost(Duration.ofSeconds(10)).until(() -> count.get() >= 1_000);
            double elapsedS = (System.nanoTime() - t0) / 1e9;
            // The absolute schedule forbids running ahead; the tolerance is one tick per worker.
            assertThat(count.get()).isLessThanOrEqualTo((long) (rate * elapsedS) + workers);
        }
    }

    @Test
    void errorsAreReportedAndScheduleContinues() {
        var errors = new AtomicLong();
        var runs = new AtomicLong();
        try (var load = new PacedLoad(500, 4, () -> {
            if (runs.incrementAndGet() % 2 == 0) {
                throw new IllegalStateException("boom");
            }
        }, t -> errors.incrementAndGet())) {
            await().atMost(Duration.ofSeconds(10)).until(() -> errors.get() >= 20 && runs.get() >= 40);
        }
    }

    @Test
    void closeStopsTheWorkers() {
        var count = new AtomicLong();
        var load = new PacedLoad(1_000, 4, count::incrementAndGet, t -> {
        });
        await().atMost(Duration.ofSeconds(10)).until(() -> count.get() > 10);
        load.close();
        long after = count.get();
        await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2)).until(() -> count.get() == after);
    }
}
