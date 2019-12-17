package com.github.ylgrgyq.reservoir.storage2;

public final class BadRecordException extends StorageRuntimeException {
    private static final long serialVersionUID = 1L;

    public BadRecordException(String message) {
        super(message);
    }

    public BadRecordException(Throwable cause) {
        super(cause);
    }

    public BadRecordException(String message, Throwable cause) {
        super(message, cause);
    }
}
