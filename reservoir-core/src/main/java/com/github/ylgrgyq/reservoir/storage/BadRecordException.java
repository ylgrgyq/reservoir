package com.github.ylgrgyq.reservoir.storage;

import com.github.ylgrgyq.reservoir.StorageException;

import javax.annotation.Nullable;

public final class BadRecordException extends StorageException {
    private static final long serialVersionUID = -2307908210663868336L;
    @Nullable
    private final RecordType type;

    public BadRecordException(String s) {
        super(s);
        this.type = null;
    }

    public BadRecordException(RecordType type) {
        super();
        this.type = type;
    }

    public BadRecordException(RecordType type, String s) {
        super(s);
        this.type = type;
    }

    public BadRecordException(RecordType type, String message, Throwable cause) {
        super(message, cause);
        this.type = type;
    }

    public BadRecordException(RecordType type, Throwable cause) {
        super(cause);
        this.type = type;
    }

    @Nullable
    public RecordType getType() {
        return type;
    }
}
