package com.github.ylgrgyq.reservoir;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * A {@link ObjectQueue} implements {@link ObjectQueueConsumer} and {@link ObjectQueueProducer}.
 *
 * @param <E> the type of the element in {@link ObjectQueue}
 */
public class ObjectQueue<E> implements ObjectQueueConsumer<E>, ObjectQueueProducer<E> {
    private final ObjectQueueProducer<E> producerDelegate;
    private final ObjectQueueConsumer<E> consumerDelegate;

    ObjectQueue(ObjectQueueProducer<E> producerDelegate, ObjectQueueConsumer<E> consumerDelegate) {
        this.producerDelegate = producerDelegate;
        this.consumerDelegate = consumerDelegate;
    }

    @Override
    public CompletableFuture<Void> produce(E object) {
        requireNonNull(object, "object");
        return producerDelegate.produce(object);
    }

    @Override
    public CompletableFuture<Void> flush() {
        return producerDelegate.flush();
    }

    @Override
    public E fetch() throws InterruptedException, StorageException {
        return consumerDelegate.fetch();
    }

    @Nullable
    @Override
    public E fetch(long timeout, TimeUnit unit) throws InterruptedException, StorageException {
        requireNonNull(unit, "unit");
        return consumerDelegate.fetch(timeout, unit);
    }

    @Override
    public void commit() throws StorageException {
        consumerDelegate.commit();
    }

    @Override
    public void close() throws Exception {
        producerDelegate.close();
        consumerDelegate.close();
    }

    @Override
    public boolean closed() {
        return consumerDelegate.closed();
    }
}
