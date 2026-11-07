package io.payflow.common;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Runs a {@link PacedLoad} for the life of the Spring context. A non-daemon thread keeps the JVM alive because the
 * load threads are virtual (daemon) and these apps have no web server.
 */
public class LoadLifecycle implements SmartLifecycle {
    private static final Logger log = LoggerFactory.getLogger(LoadLifecycle.class);

    private final String name;
    private final int rate;
    private final int workers;
    private final PacedLoad.Task task;
    private final AtomicLong errors = new AtomicLong();
    private final CountDownLatch stopped = new CountDownLatch(1);
    private volatile PacedLoad load;

    public LoadLifecycle(String name, int rate, int workers, PacedLoad.Task task) {
        this.name = name;
        this.rate = rate;
        this.workers = workers;
        this.task = task;
    }

    @Override
    public void start() {
        log.info("{} starting: rate={}/s workers={}", name, rate, workers);
        load = new PacedLoad(rate, workers, task, this::onError);
        Thread keepAlive = new Thread(() -> {
            try {
                stopped.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, name + "-keepalive");
        keepAlive.setDaemon(false);
        keepAlive.start();
    }

    private void onError(Throwable t) {
        long n = errors.incrementAndGet();
        if (n == 1) {
            log.error("{} first task failure", name, t);
        } else if (n % 1000 == 0) {
            log.error("{} task failures so far: {} (latest: {})", name, n, t.toString());
        }
    }

    @Override
    public void stop() {
        PacedLoad l = load;
        if (l != null) {
            l.close();
        }
        stopped.countDown();
    }

    @Override
    public boolean isRunning() {
        return load != null && stopped.getCount() > 0;
    }
}
