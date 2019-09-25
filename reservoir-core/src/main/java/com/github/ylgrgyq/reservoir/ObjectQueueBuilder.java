package com.github.ylgrgyq.reservoir;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * A builder to build {@link ObjectQueue}, {@link ObjectQueueProducer} or {@link ObjectQueueConsumer}.
 * Usually we build {@link ObjectQueue} directly, but in some cases {@link ObjectQueueProducer} and
 * {@link ObjectQueueConsumer} may separated on different services. So if your service only use one
 * of them, you can build the one you actually use and ignore all the setters used by the other one.
 *
 * @param <E> the type of the object to insert into the building {@link ObjectQueue}
 * @param <S> the serialized type of type {@code E}. {@link ObjectQueueStorage} is
 *            using type {@code S} to store element.
 */
public final class ObjectQueueBuilder<E, S> {
    private static final ThreadFactory threadFactory = new NamedThreadFactory("reservoir-object-queue-executor-");

    /**
     * Create a new {@link ObjectQueueBuilder} with a specific {@link ObjectQueueStorage}.
     * Use {@link IdentityCodec} as the default codec.
     *
     * @param storage a {@link ObjectQueueStorage} instance to store serialized object
     * @param <E>     the type of the object to insert into the building {@link ObjectQueue}
     * @return an new instance of {@link ObjectQueueBuilder}
     */
    public static <E> ObjectQueueBuilder<E, E> newBuilder(ObjectQueueStorage<E> storage) {
        requireNonNull(storage, "storage");

        return new ObjectQueueBuilder<>(storage, new IdentityCodec<>());
    }

    /**
     * Create a new {@link ObjectQueueBuilder} with a specific {@link ObjectQueueStorage} and {@link Codec}.
     *
     * @param storage a {@link ObjectQueueStorage} instance to store serialized object
     * @param codec   a {@link Codec} instance to serialize/deserialize object in {@link ObjectQueue}
     * @param <E>     the type of the object to insert into the building {@link ObjectQueue}
     * @param <S>     the serialized type of type {@code E}. {@link ObjectQueueStorage} is
     *                using type {@code S} to store element.
     * @return an new instance of {@link ObjectQueueBuilder}
     */
    public static <E, S> ObjectQueueBuilder<E, S> newBuilder(ObjectQueueStorage<S> storage, Codec<E, S> codec) {
        requireNonNull(storage, "storage");
        requireNonNull(codec, "codec");

        return new ObjectQueueBuilder<>(storage, codec);
    }

    private ExecutorService producerExecutorService = Executors.newSingleThreadExecutor(threadFactory);
    private boolean shutdownProducerExecutorService = true;
    private int producerRingBufferSize = 512;
    private int consumerFetchBatchSize = 128;
    private boolean autoCommit = true;

    private ObjectQueueStorage<S> storage;
    private Codec<E, S> codec;

    private ObjectQueueBuilder(ObjectQueueStorage<S> storage, Codec<E, S> codec) {
        this.storage = storage;
        this.codec = codec;
    }

    /**
     * Replace the {@link ObjectQueueStorage} passed in {@link #newBuilder(ObjectQueueStorage)}
     * with a new {@link ObjectQueueStorage} instance.
     *
     * @param storage a new {@link ObjectQueueStorage} instance
     * @return this
     */
    public ObjectQueueBuilder<E, S> replaceStorage(ObjectQueueStorage<S> storage) {
        requireNonNull(storage, "storage");
        this.storage = storage;
        return this;
    }

    /**
     * Replace the default {@link IdentityCodec} or the {@link Codec} passed in
     * {@link #newBuilder(ObjectQueueStorage)} with a new {@link Codec} instance.
     *
     * @param codec a new {@link Codec} instance
     * @return this
     */
    public ObjectQueueBuilder<E, S> replaceCodec(Codec<E, S> codec) {
        requireNonNull(codec, "codec");
        this.codec = codec;
        return this;
    }

    /**
     * Used only by {@link ObjectQueueConsumer}.
     * Commit the object retrieved by calling {@link ObjectQueueConsumer#fetch()} and
     * {@link ObjectQueueConsumer#fetch(long, TimeUnit)} automatically.
     * Calling {@link ObjectQueueConsumer#commit()} will have no effect with this set.
     *
     * @param autoCommit true to auto commit any object retrieved by {@link ObjectQueueConsumer}
     * @return this
     */
    public ObjectQueueBuilder<E, S> autoCommitAfterFetch(boolean autoCommit) {
        this.autoCommit = autoCommit;
        return this;
    }

    /**
     * Used only by {@link ObjectQueueConsumer}.
     * Set how many objects you want {@link ObjectQueueConsumer} to retrieve and keep in
     * internal memory in one call on {@link ObjectQueueStorage#fetch(long, int)}.
     * <p>
     * The default value is 128.
     *
     * @param consumerFetchBatchSize how many objects to retrieve in one call
     *                               on {@link ObjectQueueStorage#fetch(long, int)}.
     * @return this
     */
    public ObjectQueueBuilder<E, S> setConsumerFetchBatchSize(int consumerFetchBatchSize) {
        if (consumerFetchBatchSize <= 0) {
            throw new IllegalArgumentException("consumerFetchBatchSize: " + consumerFetchBatchSize + " (expected: > 0)");
        }

        this.consumerFetchBatchSize = consumerFetchBatchSize;
        return this;
    }

    /**
     * Used only by {@link ObjectQueueProducer}.
     * Set the size of the internal ring buffer which is used to cumulate several write into a big
     * write to boost performance. If this ring buffer is full, {@link ObjectQueueProducer#produce(Object)}
     * will block. If this ring buffer is too large, the performance may suffer.
     * <p>
     * The default value is 512.
     *
     * @param producerRingBufferSize internal ring buffer size
     * @return this.
     */
    public ObjectQueueBuilder<E, S> setProducerRingBufferSize(int producerRingBufferSize) {
        if (producerRingBufferSize <= 0) {
            throw new IllegalArgumentException("producerRingBufferSize: " + producerRingBufferSize + " (expected: > 0)");
        }

        this.producerRingBufferSize = producerRingBufferSize;
        return this;
    }

    /**
     * Used only by {@link ObjectQueueProducer}.
     * Set an executor service which is used to complete the
     * {@link java.util.concurrent.CompletableFuture} returned by
     * {@link ObjectQueueProducer#produce(Object)}.
     * <p>
     * Please remember to shutdown the input {@code producerExecutorService} when no one use it.
     *
     * @param executorService an instance of {@link ExecutorService}
     */
    public void setProducerExecutorService(ExecutorService executorService) {
        requireNonNull(executorService, "producerExecutorService");

        this.producerExecutorService = executorService;
        this.shutdownProducerExecutorService = false;
    }

    /**
     * Build an instance of {@link ObjectQueueProducer} with parameters in
     * this {@link ObjectQueueBuilder}.
     *
     * @return a new instance of {@link ObjectQueueProducer}
     * @throws StorageException if any error happens in underlying {@link ObjectQueueStorage}
     */
    public ObjectQueueProducer<E> buildProducer() throws StorageException {
        return new DisruptorBackedObjectQueueProducer<>(this);
    }

    /**
     * Build an instance of {@link ObjectQueueConsumer} with parameters in
     * this {@link ObjectQueueBuilder}.
     *
     * @return a new instance of {@link ObjectQueueConsumer}
     * @throws StorageException if any error happens in underlying {@link ObjectQueueStorage}
     */
    public ObjectQueueConsumer<E> buildConsumer() throws StorageException {
        if (isAutoCommit()) {
            return new AutoCommitObjectQueueConsumer<>(this);
        } else {
            return new ManualCommitObjectQueueConsumer<>(this);
        }
    }

    /**
     * Build the queue with parameters in this {@link ObjectQueueBuilder}.
     *
     * @return a new instance of {@link ObjectQueue}
     * @throws StorageException if any error happens in underlying {@link ObjectQueueStorage}
     */
    public ObjectQueue<E> buildQueue() throws StorageException {
        ObjectQueueProducer<E> producer = buildProducer();
        ObjectQueueConsumer<E> consumer = buildConsumer();

        return new ObjectQueue<>(producer, consumer);
    }

    int getProducerRingBufferSize() {
        return producerRingBufferSize;
    }

    ExecutorService getProducerExecutorService() {
        return producerExecutorService;
    }

    boolean isShutdownProducerExecutorService() {
        return shutdownProducerExecutorService;
    }

    ObjectQueueStorage<S> getStorage() {
        return storage;
    }

    Codec<E, S> getCodec() {
        return codec;
    }

    int getConsumerFetchBatchSize() {
        return consumerFetchBatchSize;
    }

    private boolean isAutoCommit() {
        return autoCommit;
    }

    private static class IdentityCodec<E> implements Codec<E, E> {
        @Override
        public E serialize(E obj) {
            return obj;
        }

        @Override
        public E deserialize(E serializedObj) {
            return serializedObj;
        }
    }
}
