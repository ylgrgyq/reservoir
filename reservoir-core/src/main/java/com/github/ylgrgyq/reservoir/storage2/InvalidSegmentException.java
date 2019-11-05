package com.github.ylgrgyq.reservoir.storage2;

public class InvalidSegmentException extends StorageRuntimeException{
    public InvalidSegmentException() {
        super();
    }

    public InvalidSegmentException(String message) {
        super(message);
    }

    public InvalidSegmentException(Throwable throwable) {
        super(throwable);
    }

    public InvalidSegmentException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
