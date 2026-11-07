package io.payflow.common;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Caps a writer at TOTAL committed events across kills and restarts. The remaining budget is TOTAL minus what the
 * database already holds for the run, so a writer restarted after kill -9 continues instead of starting over, and the
 * run ends with exactly TOTAL committed events. A task that did not commit (failure, forced rollback) gives its claim back.
 * total <= 0 means unlimited.
 */
public final class TotalGate {
    private static final Logger log = LoggerFactory.getLogger(TotalGate.class);

    private final long remaining;
    private final AtomicLong claimed = new AtomicLong();
    private final AtomicBoolean finishing = new AtomicBoolean();

    public TotalGate(long total, LongSupplier alreadyCommitted) {
        this.remaining = total <= 0 ? Long.MAX_VALUE : Math.max(0, total - alreadyCommitted.getAsLong());
        if (total > 0) {
            log.info("total gate: total={} remaining={}", total, remaining);
        }
    }

    /** True if the caller may write one more event. */
    public boolean claim() {
        return claimed.incrementAndGet() <= remaining;
    }

    /** Give the claim back because the event did not commit. */
    public void release() {
        claimed.decrementAndGet();
    }

    /** True exactly once, for the caller that should shut the process down. */
    public boolean firstToFinish() {
        return finishing.compareAndSet(false, true);
    }

    /** Flush metrics for one OTLP interval, print the summary line scripts wait for, and exit 0. */
    public static void finishAsync(ConfigurableApplicationContext ctx, String name, java.util.function.Supplier<String> summary) {
        Thread.ofPlatform().start(() -> {
            try {
                Thread.sleep(6_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            String line = name + "_DONE " + summary.get();
            log.info(line);
            System.out.println(line);
            System.exit(SpringApplication.exit(ctx));
        });
    }
}
