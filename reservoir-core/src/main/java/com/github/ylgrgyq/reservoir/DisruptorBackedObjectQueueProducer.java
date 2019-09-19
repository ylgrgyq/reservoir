package com.github.ylgrgyq.reservoir;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorThreeArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

import static java.util.Objects.requireNonNull;

final class DisruptorBackedObjectQueueProducer<E, S> implements ObjectQueueProducer<E> {
    private static final Logger logger = LoggerFactory.getLogger(DisruptorBackedObjectQueueProducer.class);
    private static final ThreadFactory producerWorkerFactory = new NamedThreadFactory("reservoir-object-queue-producer-worker-");

    private final ObjectQueueStorage<S> storage;
    private final Disruptor<ProducerEvent<S>> disruptor;
    private final RingBuffer<ProducerEvent<S>> ringBuffer;
    private final EventTranslatorThreeArg<ProducerEvent<S>, S, CompletableFuture<Void>, Boolean> translator;
    private final ExecutorService executor;
    private final boolean shutdownExecutor;
    private final Codec<E, S> serializer;
    private volatile boolean closed;

    DisruptorBackedObjectQueueProducer(ObjectQueueBuilder<E, S> builder) {
        requireNonNull(builder, "builder");

        this.storage = builder.getStorage();
        this.disruptor = new Disruptor<>(ProducerEvent::new, builder.getProducerRingBufferSize(), producerWorkerFactory);
        this.disruptor.handleEventsWith(new ProduceHandler(builder.getConsumerFetchBatchSize()));
        this.disruptor.setDefaultExceptionHandler(new LogExceptionHandler<>("object-queue-producer",
                (event, ex) -> {
                    if (event.future != null) {
                        event.future.completeExceptionally(ex);
                    }
                }
        ));

        this.disruptor.start();
        this.translator = new ProducerTranslator<>();
        this.ringBuffer = disruptor.getRingBuffer();
        this.executor = builder.getProducerExecutorService();
        this.shutdownExecutor = builder.isShutdownProducerExecutorService();
        this.serializer = builder.getCodec();
    }

    @Override
    public CompletableFuture<Void> produce(E object) {
        requireNonNull(object, "object");

        if (closed) {
            return exceptionallyCompletedFuture(new IllegalStateException("producer has been closed"));
        }

        final CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            final S payload = serializer.serialize(object);
            ringBuffer.publishEvent(translator, payload, future, Boolean.FALSE);
        } catch (SerializationException ex) {
            future.completeExceptionally(ex);
        }

        return future;
    }

    @Override
    public CompletableFuture<Void> flush() {
        if (closed) {
            return exceptionallyCompletedFuture(new IllegalStateException("producer has been closed"));
        }

        return doFlush();
    }

    @Override
    public synchronized void close() throws Exception {
        if (closed) {
            return;
        }

        closed = true;

        final CompletableFuture<Void> future = doFlush();
        future.join();

        disruptor.shutdown();
        if (shutdownExecutor) {
            executor.shutdown();
        }
        storage.close();
    }

    private CompletableFuture<Void> doFlush() {
        final CompletableFuture<Void> future = new CompletableFuture<>();

        ringBuffer.publishEvent(translator, null, future, Boolean.TRUE);
        return future;
    }

    private static <T> CompletableFuture<T> exceptionallyCompletedFuture(Throwable throwable) {
        final CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(throwable);
        return future;
    }

    private final static class ProducerTranslator<S>
            implements EventTranslatorThreeArg<ProducerEvent<S>, S, CompletableFuture<Void>, Boolean> {

        @Override
        public void translateTo(ProducerEvent<S> event, long sequence, S payload, CompletableFuture<Void> future, Boolean flush) {
            event.reset();
            event.future = future;
            if (Boolean.TRUE.equals(flush)) {
                event.flush = true;
            }

            if (payload != null) {
                event.payload = payload;
            }
        }
    }

    private final static class ProducerEvent<S> {
        @Nullable
        private S payload;
        @Nullable
        private CompletableFuture<Void> future;
        private boolean flush;

        void reset() {
            payload = null;
            future = null;
            flush = false;
        }

        @Override
        public String toString() {
            return "ProducerEvent{" +
                    "payload=" + (payload instanceof byte[] ? Base64.getEncoder().encodeToString((byte[]) payload) : payload) +
                    ", flush=" + flush +
                    '}';
        }
    }

    private final class ProduceHandler implements EventHandler<ProducerEvent<S>> {
        private final int batchSize;
        private final List<S> batchPayload;
        private final List<CompletableFuture<Void>> batchFutures;

        ProduceHandler(int batchSize) {
            this.batchSize = batchSize;
            this.batchPayload = new ArrayList<>(batchSize);
            this.batchFutures = new ArrayList<>(batchSize);
        }

        @Override
        public void onEvent(ProducerEvent<S> event, long sequence, boolean endOfBatch) {
            assert event.future != null;

            if (event.flush) {
                if (!batchPayload.isEmpty()) {
                    flush();
                }
                executor.execute(() -> event.future.complete(null));
            } else {
                batchPayload.add(event.payload);
                batchFutures.add(event.future);
                if (batchPayload.size() >= batchSize || endOfBatch) {
                    flush();
                }
            }

            assert batchPayload.size() == batchFutures.size() :
                    "batchPayload: " + batchPayload.size() + " batchFutures: " + batchFutures.size();
        }

        private void flush() {
            try {
                storage.store(batchPayload);

                completeFutures(batchFutures);
            } catch (StorageException ex) {
                completeFutures(batchFutures, ex);
            }

            batchPayload.clear();
            batchFutures.clear();
        }

        private void completeFutures(List<CompletableFuture<Void>> futures) {
            for (final CompletableFuture<Void> future : futures) {
                try {
                    executor.execute(() -> future.complete(null));
                } catch (Exception ex) {
                    logger.error("Submit complete future task failed", ex);
                }
            }
        }

        private void completeFutures(List<CompletableFuture<Void>> futures, Throwable t) {
            for (final CompletableFuture<Void> future : futures) {
                try {
                    executor.execute(() -> future.completeExceptionally(t));
                } catch (Exception ex) {
                    logger.error("Submit complete future task failed", ex);
                }
            }
        }
    }
}
