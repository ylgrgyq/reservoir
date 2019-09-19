package com.github.ylgrgyq.reservoir;

import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import static com.github.ylgrgyq.reservoir.TestingUtils.numberStringBytes;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

public class DisruptorBackedObjectQueueProducerTest {
    private final TestingStorage<TestingPayload> storage = new TestingStorage<>();
    private final ObjectQueueBuilder<TestingPayload, TestingPayload> builder = ObjectQueueBuilder.newBuilder(storage);

    @Before
    public void setUp() {
        storage.clear();
    }

    @Test
    public void simpleProduceAndFlush() throws Exception {
        final ObjectQueueProducer<TestingPayload> producer = builder.buildProducer();

        final List<TestingPayload> producedPayload = new ArrayList<>();
        final ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 64; i++) {
            final TestingPayload payload = new TestingPayload(numberStringBytes(i));
            final CompletableFuture<Void> f = producer.produce(payload);
            assertThat(f).isNotNull();
            producedPayload.add(payload);
            futures.add(f);
        }

        CompletableFuture<Void> flushFuture = producer.flush();
        await().until(flushFuture::isDone);

        assertThat(futures).allSatisfy(CompletableFuture::isDone);

        final List<TestingPayload> payloads = storage
                .getProdcedPayloads()
                .stream()
                .map(SerializedObjectWithId::getSerializedObject)
                .collect(Collectors.toList());
        assertThat(producedPayload).isEqualTo(payloads);
    }

    @Test
    public void simpleProduceAndAutoFlush() throws Exception {
        final ObjectQueueProducer<TestingPayload> producer = builder.buildProducer();
        final List<TestingPayload> producedPayloads = new ArrayList<>();
        final ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 1024; i++) {
            final TestingPayload payload = new TestingPayload(numberStringBytes(i));
            CompletableFuture<Void> f = producer.produce(payload);
            assertThat(f).isNotNull();
            futures.add(f);
            producedPayloads.add(payload);
        }

        await().until(() -> allAsList(futures).isDone());
        assertThat(futures).allSatisfy(CompletableFuture::isDone);

        final List<TestingPayload> payloads = storage.getProdcedPayloads()
                .stream()
                .map(SerializedObjectWithId::getSerializedObject)
                .collect(Collectors.toList());
        assertThat(producedPayloads).isEqualTo(payloads);
    }

    @Test
    public void flushAllProducedObjectOnClose() throws Exception {
        final ObjectQueueProducer<TestingPayload> producer = builder
                .setConsumerFetchBatchSize(5)
                .replaceStorage(new AbstractTestingStorage<TestingPayload>() {
                    @Override
                    public void store(List<TestingPayload> batch) throws StorageException {
                        try {
                            Thread.sleep(200);
                        } catch (Exception ex) {
                            throw new StorageException(ex);
                        }
                    }
                }).buildProducer();

        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final TestingPayload payload = new TestingPayload(numberStringBytes(i));
            CompletableFuture<Void> f = producer.produce(payload);
            assertThat(f).isNotNull();
            futures.add(f);
        }

        producer.close();
        assertThat(futures).allSatisfy(CompletableFuture::isDone);
    }

    @Test
    public void produceAfterClose() throws Exception {
        final ObjectQueueProducer<TestingPayload> producer = builder.buildProducer();

        producer.close();

        assertThatThrownBy(() -> producer.produce(new TestingPayload()).join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("producer has been closed");
    }

    @Test
    public void produceWhenSerializeElementFailed() throws Exception {
        final ObjectQueueProducer<TestingPayload> producer = builder
                .replaceCodec(new BadTestingPayloadCodec<>())
                .buildProducer();

        assertThatThrownBy(() -> producer.produce(new TestingPayload()).join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(SerializationException.class);
    }

    @Test
    public void storageThrowsStorageException() throws Exception {
        ObjectQueueProducer<TestingPayload> producer = builder.replaceStorage(new AbstractTestingStorage<TestingPayload>() {
            @Override
            public void store(List<TestingPayload> batch) throws StorageException {
                throw new StorageException("deliberate store failed");
            }
        }).buildProducer();

        TestingPayload payload = new TestingPayload(("Hello").getBytes(StandardCharsets.UTF_8));
        CompletableFuture<Void> f = producer.produce(payload);
        await().until(f::isDone);
        assertThat(f).hasFailedWithThrowableThat().hasMessageContaining("store failed").isInstanceOf(StorageException.class);
    }

    @Test
    public void storageThrowsOtherException() throws Exception {
        ObjectQueueProducer<TestingPayload> producer = builder.replaceStorage(new AbstractTestingStorage<TestingPayload>() {
            @Override
            public void store(List<TestingPayload> batch) {
                throw new RuntimeException("deliberate store failed");
            }
        }).buildProducer();

        TestingPayload payload = new TestingPayload(("Hello").getBytes(StandardCharsets.UTF_8));
        CompletableFuture<Void> f = producer.produce(payload);
        await().until(f::isDone);
        assertThat(f).hasFailedWithThrowableThat().hasMessageContaining("store failed").isInstanceOf(RuntimeException.class);
    }

    public static <T> CompletableFuture<List<T>> allAsList(
            List<? extends CompletionStage<? extends T>> stages) {
        // We use traditional for-loops instead of streams here for performance reasons,
        // see AllAsListBenchmark

        @SuppressWarnings("unchecked") // generic array creation
        final CompletableFuture<? extends T>[] all = new CompletableFuture[stages.size()];
        for (int i = 0; i < stages.size(); i++) {
            all[i] = stages.get(i).toCompletableFuture();
        }
        return CompletableFuture.allOf(all)
                .thenApply(ignored -> {
                    final List<T> result = new ArrayList<>(all.length);
                    for (int i = 0; i < all.length; i++) {
                        result.add(all[i].join());
                    }
                    return result;
                });
    }
}