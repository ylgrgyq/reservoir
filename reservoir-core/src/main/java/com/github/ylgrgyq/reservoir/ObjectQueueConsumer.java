package com.github.ylgrgyq.reservoir;

import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;

public interface ObjectQueueConsumer<E> extends AutoCloseable {
    /**
     * Retrieves the head of this queue, waiting if necessary until an
     * element becomes available.
     *
     * @return the fetched object
     * @throws InterruptedException if interrupted while waiting
     * @throws StorageException     if any error happens in underlying storage
     */
    E fetch() throws InterruptedException, StorageException;

    /**
     * Retrieves the head of this queue, waiting up to the specified wait
     * time if necessary for an element to become available.
     *
     * @param timeout how long to wait before giving up, in units of
     *                {@code unit}
     * @param unit    a {@code TimeUnit} determining how to interpret the
     *                {@code timeout} parameter
     * @return the head of this queue, or {@code null} if the specified waiting time
     *         elapses before an element is available
     * @throws InterruptedException if interrupted while waiting
     * @throws StorageException     if any error happens in underlying storage
     */
    @Nullable
    E fetch(long timeout, TimeUnit unit) throws InterruptedException, StorageException;

    /**
     * Remove the head of this queue. Please call this method only after
     * actually fetched an object from this queue.
     *
     * @throws java.util.NoSuchElementException if {@link #fetch()} or {@link #fetch(long, TimeUnit)}
     *                                          is not called before
     * @throws StorageException                 if any error happens in underlying storage
     */
    void commit() throws StorageException;

    /**
     * Check if the consumer is closed.
     *
     * @return true if this consumer has closed
     */
    boolean closed();
}
