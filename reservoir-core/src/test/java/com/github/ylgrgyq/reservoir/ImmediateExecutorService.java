package com.github.ylgrgyq.reservoir;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ImmediateExecutorService extends AbstractExecutorService {
    private final AtomicBoolean shutdown = new AtomicBoolean();

    @Override
    public void shutdown() {
        shutdown.compareAndSet(false, true);
    }

    @Override
    public List<Runnable> shutdownNow() {
        shutdown.compareAndSet(false, true);
        return Collections.emptyList();
    }

    @Override
    public boolean isShutdown() {
        return shutdown.get();
    }

    @Override
    public boolean isTerminated() {
        return shutdown.get();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
        shutdown.compareAndSet(false, true);
        return true;
    }

    @Override
    public void execute(Runnable command) {
        if (!shutdown.get()) {
            command.run();
        }
    }
}
