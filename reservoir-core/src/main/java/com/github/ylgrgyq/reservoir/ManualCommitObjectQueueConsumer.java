package com.github.ylgrgyq.reservoir;

import javax.annotation.Nullable;
import java.util.Base64;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Objects.requireNonNull;

final class ManualCommitObjectQueueConsumer<E, S> implements ObjectQueueConsumer<E> {
    private final ObjectQueueStorage<S> storage;
    private final BlockingQueue<DeserializedObjectWithId<E>> queue;
    private final ReentrantLock lock;
    private final int batchSize;
    private final Codec<E, S> deserializer;
    private long lastCommittedId;

    private volatile boolean closed;

    ManualCommitObjectQueueConsumer(ObjectQueueBuilder<E, S> builder) throws StorageException {
        requireNonNull(builder, "builder");

        this.storage = builder.getStorage();
        this.batchSize = builder.getConsumerFetchBatchSize();
        this.queue = new ArrayBlockingQueue<>(2 * this.batchSize);
        this.lastCommittedId = storage.getLastCommittedId();
        this.deserializer = builder.getCodec();
        this.lock = new ReentrantLock();
    }

    @Override
    public E fetch() throws InterruptedException, StorageException {
        DeserializedObjectWithId<E> payload;
        while ((payload = queue.peek()) == null) {
            blockFetchFromStorage(0, TimeUnit.NANOSECONDS);
        }

        return payload.object;
    }

    @Nullable
    @Override
    public E fetch(long timeout, TimeUnit unit) throws InterruptedException, StorageException {
        requireNonNull(unit);

        DeserializedObjectWithId<E> payload;
        while ((payload = queue.peek()) == null) {
            if (!blockFetchFromStorage(timeout, unit)) {
                break;
            }
        }

        return payload == null ? null : payload.object;
    }

    @Override
    public void commit() throws StorageException {
        final DeserializedObjectWithId<E> payload = queue.poll();
        if (payload == null) {
            throw new NoSuchElementException();
        }

        lock.lock();
        try {
            final long id = payload.id;
            if (id > lastCommittedId) {
                lastCommittedId = id;
                storage.commitId(id);
            }
        } finally {
            lock.unlock();
        }
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
            final long lastId = lastCommittedId;
            final List<? extends SerializedObjectWithId<S>> payloads;
            if (timeout == 0) {
                payloads = storage.fetch(lastId, batchSize);
            } else {
                payloads = storage.fetch(lastId, batchSize, timeout, unit);
            }

            for (SerializedObjectWithId<S> p : payloads) {
                final S serializeP = p.getSerializedObject();
                try {
                    final E pObj = deserializer.deserialize(serializeP);
                    queue.put(new DeserializedObjectWithId<>(p.getId(), pObj));
                } catch (InterruptedException ex) {
                    throw ex;
                } catch (Exception ex) {
                    String msg = "deserialize object with id: " + p.getId() + " failed. Content is: " +
                            (serializeP instanceof byte[] ?
                                    Base64.getEncoder().encodeToString((byte[]) serializeP) + " (Base64)" :
                                    serializeP);
                    throw new DeserializationException(msg, ex);
                }
            }

            return !payloads.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    private static final class DeserializedObjectWithId<E> {
        private final E object;
        private final long id;

        DeserializedObjectWithId(long id, E object) {
            this.object = object;
            this.id = id;
        }
    }
}
