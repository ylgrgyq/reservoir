package com.github.ylgrgyq.reservoir;

import java.util.List;
import java.util.concurrent.*;

public class DelayedSingleThreadExecutorService extends AbstractExecutorService {
    private static final ThreadFactory threadFactory = new NamedThreadFactory("delayed-single-thread-pool-");
    private final long defaultDelayedMillis;
    private final ExecutorService executorService;
    private final Semaphore semaphore;

    public DelayedSingleThreadExecutorService() {
        this(0, 0, TimeUnit.MILLISECONDS);
    }

    public DelayedSingleThreadExecutorService(long defaultDelayed, TimeUnit unit) {
        this(0, defaultDelayed, unit);
    }

    public DelayedSingleThreadExecutorService(int noDelayForFirstNTasks, long defaultDelayed, TimeUnit unit) {
        this.executorService = Executors.newSingleThreadScheduledExecutor(threadFactory);
        this.defaultDelayedMillis = unit.toMillis(defaultDelayed);
        this.semaphore = new Semaphore(Integer.MAX_VALUE);
        this.semaphore.drainPermits();
        this.semaphore.release(noDelayForFirstNTasks);
    }

    @Override
    public void shutdown() {
        executorService.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return executorService.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return executorService.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return executorService.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
        return true;
    }

    public void letOnePass() {
        semaphore.release(1);
    }

    @Override
    public void execute(Runnable command) {
        if (semaphore.tryAcquire()) {
            command.run();
        } else {
            executorService.execute(() -> {
                try {
                    semaphore.tryAcquire(defaultDelayedMillis, TimeUnit.MILLISECONDS);
                    command.run();
                } catch (InterruptedException ex) {
                    // ignore
                }
            });
        }
    }
}
