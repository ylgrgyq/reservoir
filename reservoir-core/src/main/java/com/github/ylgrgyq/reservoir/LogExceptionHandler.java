package com.github.ylgrgyq.reservoir;

import com.lmax.disruptor.ExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

final class LogExceptionHandler<T> implements ExceptionHandler<T> {
    private static final Logger logger = LoggerFactory.getLogger(LogExceptionHandler.class);

    public interface OnEventException<T> {

        void onException(T event, Throwable ex);
    }

    private final String name;
    @Nullable
    private final OnEventException<T> onEventException;

    LogExceptionHandler(String name) {
        this(name, null);
    }

    LogExceptionHandler(String name, @Nullable OnEventException<T> onEventException) {
        this.name = name;
        this.onEventException = onEventException;
    }

    @Override
    public void handleOnStartException(Throwable ex) {
        logger.error("Start disruptor: {} failed.", name, ex);
    }

    @Override
    public void handleOnShutdownException(Throwable ex) {
        logger.error("Shutdown disruptor: {} failed.", name, ex);

    }

    @Override
    public void handleEventException(Throwable ex, long sequence, T event) {
        logger.error("Handle event: {} on disruptor: {} failed.", event, name, ex);
        if (this.onEventException != null) {
            this.onEventException.onException(event, ex);
        }
    }
}