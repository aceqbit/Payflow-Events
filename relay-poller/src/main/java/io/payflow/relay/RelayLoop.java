package io.payflow.relay;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Drives OutboxRelay.pollOnce(): short backoff after a full batch, longer after a partial or empty one. The thread is a
 * non-daemon platform thread so it also keeps the JVM alive. A failed batch was rolled back, so it is simply retried
 * after a pause; nothing is skipped.
 */
public class RelayLoop implements SmartLifecycle {
    private static final Logger log = LoggerFactory.getLogger(RelayLoop.class);
    private static final long FAILURE_PAUSE_MS = 1_000;

    private final OutboxRelay relay;
    private final long busyMs;
    private final long idleMs;
    private volatile boolean running;
    private Thread thread;

    public RelayLoop(OutboxRelay relay, long busyMs, long idleMs) {
        this.relay = relay;
        this.busyMs = busyMs;
        this.idleMs = idleMs;
    }

    @Override
    public void start() {
        running = true;
        thread = new Thread(this::loop, "relay-poller");
        thread.setDaemon(false);
        thread.start();
        log.info("relay-poller started: batch={} busy={}ms idle={}ms", relay.batchSize(), busyMs, idleMs);
    }

    private void loop() {
        long failures = 0;
        while (running) {
            long pause;
            try {
                int n = relay.pollOnce();
                pause = n >= relay.batchSize() ? busyMs : idleMs;
            } catch (RuntimeException e) {
                if (++failures == 1 || failures % 100 == 0) {
                    log.error("poll failed, batch rolled back (failures={})", failures, e);
                }
                pause = FAILURE_PAUSE_MS;
            }
            try {
                Thread.sleep(pause);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    @Override
    public void stop() {
        running = false;
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
