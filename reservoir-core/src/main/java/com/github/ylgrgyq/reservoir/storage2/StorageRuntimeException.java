package com.github.ylgrgyq.reservoir.storage2;

public class StorageRuntimeException extends RuntimeException{
    public StorageRuntimeException() {
        super();
    }

    public StorageRuntimeException(String message) {
        super(message);
    }

    public StorageRuntimeException(Throwable throwable) {
        super(throwable);
    }

    public StorageRuntimeException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
