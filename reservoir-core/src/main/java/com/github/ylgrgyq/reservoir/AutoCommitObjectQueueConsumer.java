package com.github.ylgrgyq.reservoir;

import javax.annotation.Nullable;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Objects.requireNonNull;

final class AutoCommitObjectQueueConsumer<E, S> implements ObjectQueueConsumer<E> {
    private final ObjectQueueStorage<S> storage;
    private final BlockingQueue<E> queue;
    private final int batchSize;
    private final ReentrantLock lock;
    private final Codec<E, S> deserializer;
    private long lastCommittedId;

    private volatile boolean closed;

    AutoCommitObjectQueueConsumer(ObjectQueueBuilder<E, S> builder) throws StorageException {
        requireNonNull(builder, "builder");

        this.storage = builder.getStorage();
        this.batchSize = builder.getConsumerFetchBatchSize();
        this.queue = new ArrayBlockingQueue<>(2 * this.batchSize);
        this.lock = new ReentrantLock();
        this.lastCommittedId = storage.getLastCommittedId();
        this.deserializer = builder.getCodec();
    }

    @Override
    public E fetch() throws InterruptedException, StorageException {
        E obj;
        while ((obj = queue.poll()) == null) {
            blockFetchFromStorage(0, TimeUnit.NANOSECONDS);
        }

        return obj;
    }

    @Nullable
    @Override
    public E fetch(long timeout, TimeUnit unit) throws InterruptedException, StorageException {
        requireNonNull(unit);

        E obj;
        while ((obj = queue.poll()) == null) {
            if (!blockFetchFromStorage(timeout, unit)) {
                break;
            }
        }

        return obj;
    }

    @Override
    public void commit() {
        // id of the object at the head of the queue is already committed when it's fetched from the storage
    }

    @Override
    public boolean closed() {
        return closed;
    }

    @Override
    public void close() throws Exception {
        closed = true;

        storage.close();
    }

    private boolean blockFetchFromStorage(long timeout, TimeUnit unit) throws InterruptedException, StorageException {
        if (closed) {
            throw new InterruptedException("consumer closed");
        }

        if (!queue.isEmpty()) {
            return true;
        }

        lock.lock();
        try {
            long lastId = lastCommittedId;
            final List<? extends SerializedObjectWithId<S>> payloads;
            if (timeout == 0) {
                payloads = storage.fetch(lastId, batchSize);
            } else {
                payloads = storage.fetch(lastId, batchSize, timeout, unit);
            }

            if (!payloads.isEmpty()) {
                for (SerializedObjectWithId<S> p : payloads) {
                    final S serializeP = p.getSerializedObject();
                    try {
                        final E pObj = deserializer.deserialize(serializeP);
                        queue.put(pObj);
                    } catch (InterruptedException ex) {
                        throw ex;
                    } catch (Exception ex) {
                        String msg = "deserialize object with id: " + p.getId() + " failed. Content is: " +
                                (serializeP instanceof byte[] ?
                                        Base64.getEncoder().encodeToString((byte[]) serializeP) + " (Base64)" :
                                        serializeP);
                        throw new DeserializationException(msg, ex);
                    }

                    lastId = p.getId();
                }

                storage.commitId(lastId);
                lastCommittedId = lastId;
                return true;
            } else {
                return false;
            }
        } finally {
            lock.unlock();
        }
    }
}
