package com.github.ylgrgyq.reservoir.storage2;

public class EmptySegmentException extends StorageRuntimeException {
    public EmptySegmentException() {
        super();
    }

    public EmptySegmentException(String message) {
        super(message);
    }

    public EmptySegmentException(Throwable throwable) {
        super(throwable);
    }

    public EmptySegmentException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
