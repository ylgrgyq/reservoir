package com.github.ylgrgyq.reservoir;

import java.util.concurrent.CompletableFuture;

public interface ObjectQueueProducer<E> extends AutoCloseable {
    /**
     * Inserts the specified object into this queue, waiting if necessary
     * when the downstream storage is too slow or running out of space.
     *
     * @param object The object to insert
     * @return a future which will be completed when the object is safely
     *         inserted or some exceptions encountered
     */
    CompletableFuture<Void> produce(E object);

    /**
     * Flush any pending object stayed on the internal buffer.
     *
     * @return a future which will be completed when the flush task is done
     */
    CompletableFuture<Void> flush();
}
