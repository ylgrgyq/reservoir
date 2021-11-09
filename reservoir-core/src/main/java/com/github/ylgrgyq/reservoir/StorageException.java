package com.github.ylgrgyq.reservoir;

import java.io.IOException;

/**
 * A {@link StorageException} encapsulates the error of an operation within {@link ObjectQueueStorage}.
 * This exception type is used to describe an internal error in {@link ObjectQueueStorage}.
 */
public class StorageException extends IOException {
    public StorageException() {
        super();
    }

    public StorageException(String message) {
        super(message);
    }

    public StorageException(Throwable throwable) {
        super(throwable);
    }

    public StorageException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
