package com.github.ylgrgyq.reservoir;

/**
 * A {@link DeserializationException} encapsulates the error occurred when deserialize
 * an object by calling {@link Codec#deserialize(Object)}.
 */
public final class DeserializationException extends StorageException {
    public DeserializationException() {
        super();
    }

    public DeserializationException(String message) {
        super(message);
    }

    public DeserializationException(Throwable throwable) {
        super(throwable);
    }

    public DeserializationException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
