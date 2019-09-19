package com.github.ylgrgyq.reservoir;

/**
 * A {@link SerializationException} encapsulates the error occurred when serialize
 * an object by calling {@link Codec#serialize(Object)}.
 */
public final class SerializationException extends StorageException {
    public SerializationException() {
        super();
    }

    public SerializationException(String message) {
        super(message);
    }

    public SerializationException(Throwable throwable) {
        super(throwable);
    }

    public SerializationException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
