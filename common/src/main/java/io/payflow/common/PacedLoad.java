package io.payflow.common;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

/**
 * Fixed-rate load driver on virtual threads. {@code workers} threads each run the task at rate/workers per second on an
 * absolute schedule, so a slow task is caught up but never bursts beyond one second of backlog. A failing task is
 * reported to {@code onError} and the schedule continues (this is a load driver, not a listener).
 */
public final class PacedLoad implements AutoCloseable {
    @FunctionalInterface
    public interface Task {
        void run() throws Exception;
    }

    private static final long MAX_BACKLOG_NS = 1_000_000_000L;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Thread[] threads;

    public PacedLoad(int rate, int workers, Task task, Consumer<Throwable> onError) {
        if (rate <= 0 || workers <= 0) {
            throw new IllegalArgumentException("rate and workers must be > 0");
        }
        long intervalNs = 1_000_000_000L * workers / rate;
        long start = System.nanoTime();
        threads = new Thread[workers];
        for (int i = 0; i < workers; i++) {
            long first = start + intervalNs * i / workers;
            threads[i] = Thread.ofVirtual().name("load-" + i).start(() -> loop(first, intervalNs, task, onError));
        }
    }

    private void loop(long next, long intervalNs, Task task, Consumer<Throwable> onError) {
        while (running.get()) {
            long wait = next - System.nanoTime();
            if (wait > 0) {
                LockSupport.parkNanos(wait);
                continue;
            }
            if (-wait > MAX_BACKLOG_NS) {
                next = System.nanoTime();
            }
            try {
                task.run();
            } catch (Throwable t) {
                onError.accept(t);
            }
            next += intervalNs;
        }
    }

    @Override
    public void close() {
        running.set(false);
        for (Thread t : threads) {
            LockSupport.unpark(t);
            try {
                t.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
