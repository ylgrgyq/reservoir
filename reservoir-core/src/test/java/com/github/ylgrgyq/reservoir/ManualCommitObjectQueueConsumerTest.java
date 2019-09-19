package com.github.ylgrgyq.reservoir;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

public class ManualCommitObjectQueueConsumerTest {
    private final TestingStorage<TestingPayload> storage = new TestingStorage<>();
    private final ObjectQueueBuilder<TestingPayload, TestingPayload> builder = ObjectQueueBuilder.newBuilder(storage)
            .autoCommitAfterFetch(false);

    @Before
    public void setUp() {
        storage.clear();
    }

    @Test
    public void simpleFetch() throws Exception {
        final TestingPayload first = new TestingPayload("first");
        final TestingPayload second = new TestingPayload("second");

        storage.store(Arrays.asList(first, second));

        final ObjectQueueConsumer<TestingPayload> consumer = builder.buildConsumer();

        TestingPayload payload = consumer.fetch();
        assertThat(consumer.fetch()).isSameAs(payload).isEqualTo(first);
        consumer.commit();
        assertThat(consumer.fetch()).isEqualTo(second).isNotEqualTo(first);
        consumer.close();
    }

    @Test
    public void commitWithoutFetch() throws Exception {
        ObjectQueueConsumer<TestingPayload> consumer = builder.buildConsumer();

        assertThatThrownBy(consumer::commit).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    public void fetchAfterClose() throws Exception {
        ObjectQueueConsumer<TestingPayload> consumer = builder.buildConsumer();
        consumer.close();
        assertThatThrownBy(consumer::fetch).isInstanceOf(InterruptedException.class);
    }

    @Test
    public void deserializeObjectFailed() throws Exception {
        final ObjectQueueConsumer<TestingPayload> consumer = builder
                .replaceCodec(new BadTestingPayloadCodec<>())
                .buildConsumer();

        TestingPayload first = new TestingPayload("first");
        storage.store(Collections.singletonList(first));

        assertThatThrownBy(consumer::fetch)
                .isInstanceOf(DeserializationException.class)
                .hasMessageContaining("deserialize object with id: 1 failed. Content is: ");
        consumer.close();
    }

    @Test
    public void timeoutOnFetch() throws Exception {
        ObjectQueueConsumer<TestingPayload> consumer = builder.buildConsumer();

        assertThat(consumer.fetch(100, TimeUnit.MILLISECONDS)).isNull();
        consumer.close();
    }

    @Test
    public void blockFetch() throws Exception {
        final TestingPayload first = new TestingPayload("first");
        final ObjectQueueConsumer<TestingPayload> consumer = builder.buildConsumer();
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CompletableFuture<TestingPayload> f = CompletableFuture.supplyAsync(() -> {
            try {
                barrier.await();
                return consumer.fetch();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });
        barrier.await();
        storage.store(Collections.singletonList(first));

        await().until(f::isDone);
        assertThat(f).isCompletedWithValue(first);
        consumer.commit();
        assertThat(storage.getLastCommittedId()).isEqualTo(1);

        consumer.close();
    }
}