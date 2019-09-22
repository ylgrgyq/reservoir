package com.github.ylgrgyq.reservoir;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A storage used by {@link ObjectQueueConsumer} or {@link ObjectQueueProducer} to
 * store/fetch the serialized objects.
 * The implementation of this interface should provide the safety for concurrent access
 * from multiple threads without any external synchronization.
 */
public interface ObjectQueueStorage<S> extends AutoCloseable {
    /**
     * Store a list of serialized object to storage.
     *
     * @param batch the list of serialized object.
     * @throws StorageException if any error happens in underlying storage
     */
    void store(List<S> batch) throws StorageException;

    /**
     * Save commit id of {@link ObjectQueueConsumer} to storage.
     *
     * @param id the id to commit
     * @throws StorageException if any error happens in underlying storage
     */
    void commitId(long id) throws StorageException;

    /**
     * Get the latest id successfully passed to {@link #commitId(long)}.
     * Used by {@link ObjectQueueConsumer} to prevent it from consume an object which
     * has committed.
     *
     * @return the latest saved commit id
     * @throws StorageException if any error happens in underlying storage
     */
    long getLastCommittedId() throws StorageException;

    /**
     * Retrieves a list of serialized objects with their assigned id starting after
     * {@code fromId} from this storage, waiting if necessary until an
     * element becomes available.
     * <p>
     * Returned at most {@code limit} objects and at least one object was fetched.
     *
     * @param fromId the id from which to retrieve, exclusive.
     * @param limit  the maximum size of the returned list.
     * @return A list of serialized objects with their assigned id. Every {@link SerializedObjectWithId}
     *         in this list should have an id greater than {@code fromId}
     * @throws InterruptedException if interrupted while waiting
     * @throws StorageException     if any error happens in underlying storage
     */
    List<SerializedObjectWithId<S>> fetch(long fromId, int limit)
            throws InterruptedException, StorageException;

    /**
     * Retrieves a list of serialized objects with their assigned id starting after
     * {@code fromId} from this storage, waiting up to the specified wait
     * time if necessary for an element to become available.
     * <p>
     * Returned at most {@code limit} objects when at least one object was fetched or
     * the timeout was reached.
     *
     * @param fromId  the id from which to retrieve, exclusive.
     * @param limit   the maximum size of the returned list.
     * @param timeout how long to wait before giving up, in units of
     *                {@code unit}
     * @param unit    a {@code TimeUnit} determining how to interpret the
     *                {@code timeout} parameter
     * @return A list of serialized objects with their assigned id. Every {@link SerializedObjectWithId}
     *         in this list should have an id greater than {@code fromId}
     * @throws InterruptedException if interrupted while waiting
     * @throws StorageException     if any error happens in underlying storage
     */
    List<SerializedObjectWithId<S>> fetch(long fromId, int limit, long timeout, TimeUnit unit)
            throws InterruptedException, StorageException;
}
