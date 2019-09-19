package com.github.ylgrgyq.reservoir;

import com.github.ylgrgyq.reservoir.AutomaticObjectQueueConsumer.ConsumeObjectHandler;
import com.github.ylgrgyq.reservoir.AutomaticObjectQueueConsumer.ConsumeObjectListener;
import com.github.ylgrgyq.reservoir.AutomaticObjectQueueConsumer.HandleFailedStrategy;
import org.assertj.core.api.Condition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static com.github.ylgrgyq.reservoir.TestingUtils.numberStringBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class AutomaticObjectQueueConsumerTest {
    private final Condition<Throwable> runtimeException = new Condition<>(e -> e instanceof RuntimeException, "RuntimeException");
    private final TestingConsumeObjectListener<TestingPayload> listener = new TestingConsumeObjectListener<>();
    private TestingStorage<TestingPayload> storage;
    private Executor listenerExecutor;
    private ObjectQueue<TestingPayload> queue;

    @Before
    public void setUp() throws Exception {
        storage = new TestingStorage<>();
        listener.clear();
        listenerExecutor = Executors.newSingleThreadExecutor();
        queue = ObjectQueueBuilder.newBuilder(storage)
                .autoCommitAfterFetch(false)
                .buildQueue();
    }

    @After
    public void tearDown() throws Exception {
        queue.close();
    }

    @Test
    public void simpleConsume() throws Exception {
        final List<TestingPayload> storedPayload = generateTestingPayload(64, true);
        final AlwaysSuccessConsumeObjectHandler<TestingPayload> handler = new AlwaysSuccessConsumeObjectHandler<>();
        final AutomaticObjectQueueConsumer<TestingPayload> consumer = new AutomaticObjectQueueConsumer<>(queue, handler);

        await().until(() -> handler.getReceivedObjects().size() == storedPayload.size());
        assertThat(handler.getReceivedObjects())
                .isEqualTo(storedPayload);
        consumer.close();
    }

    @Test
    public void consumeInvalidObject() throws Exception {
        final List<TestingPayload> storedPayload = generateTestingPayload(64, false);

        final AlwaysSuccessConsumeObjectHandler<TestingPayload> handler = new AlwaysSuccessConsumeObjectHandler<>();
        final AutomaticObjectQueueConsumer<TestingPayload> consumer = new AutomaticObjectQueueConsumer<>(queue,
                handler,
                listenerExecutor,
                Collections.singletonList(listener));

        await().until(() -> listener.getInvalidObjects().size() == storedPayload.size());
        assertThat(listener.getInvalidObjects())
                .isEqualTo(storedPayload);
        consumer.close();
    }

    @Test
    public void shutdownAfterConsumeObjectFailed() {
        generateTestingPayload(1, true);

        final AutomaticObjectQueueConsumer<TestingPayload> consumer = new AutomaticObjectQueueConsumer<>(queue,
                new AlwaysThrowsExceptionHandler<TestingPayload>() {
                    @Override
                    public HandleFailedStrategy onHandleObjectFailed(TestingPayload obj, Throwable throwable) {
                        return HandleFailedStrategy.SHUTDOWN;
                    }
                });

        await().until(consumer::closed);
    }

    @Test
    public void retryAfterConsumeObjectFailed() throws Exception {
        generateTestingPayload(1, true);

        final RetryAfterFailedHandler<TestingPayload> handler = new RetryAfterFailedHandler<>(10);
        final AutomaticObjectQueueConsumer<TestingPayload> consumer = new AutomaticObjectQueueConsumer<>(queue, handler);

        await().until(() -> handler.getHandleTimes() == 10);
        consumer.close();
    }

    @Test
    public void ignoreAfterConsumeObjectFailed() throws Exception {
        final List<TestingPayload> storedObjects = generateTestingPayload(64, true);

        final List<TestingPayload> handledPayloads = new ArrayList<>();
        final AutomaticObjectQueueConsumer<TestingPayload> consumer = new AutomaticObjectQueueConsumer<>(queue,
                new AlwaysThrowsExceptionHandler<TestingPayload>() {
                    @Override
                    public HandleFailedStrategy onHandleObjectFailed(TestingPayload obj, Throwable throwable) {
                        handledPayloads.add(obj);
                        return HandleFailedStrategy.IGNORE;
                    }
                });

        await().until(() -> handledPayloads.size() == storedObjects.size());
        assertThat(handledPayloads)
                .isEqualTo(storedObjects);
        consumer.close();
    }

    @Test
    public void onHandleObjectFailedThrowsException() {
        generateTestingPayload(1, true);

        final AutomaticObjectQueueConsumer<TestingPayload> consumer = new AutomaticObjectQueueConsumer<>(
                queue,
                new AlwaysThrowsExceptionHandler<TestingPayload>() {});

        await().until(consumer::closed);
    }

    @Test
    public void onHandleObjectFailedReturnsNull() {
        generateTestingPayload(1, true);

        final AutomaticObjectQueueConsumer<TestingPayload> consumer = new AutomaticObjectQueueConsumer<>(
                queue,
                new AlwaysThrowsExceptionHandler<TestingPayload>() {
                    @Override
                    @SuppressWarnings("ConstantConditions")
                    public HandleFailedStrategy onHandleObjectFailed(TestingPayload obj, Throwable throwable) {
                        return null;
                    }
                });

        await().until(consumer::closed);
    }

    @Test
    public void testListenerOnSuccessObjectsCalled() throws Exception {
        final List<TestingPayload> storedPayload = generateTestingPayload(64, true);
        final AutomaticObjectQueueConsumer<TestingPayload> consumer = new AutomaticObjectQueueConsumer<>(
                queue,
                new AlwaysSuccessConsumeObjectHandler<>(),
                listenerExecutor,
                Collections.singletonList(listener));


        await().until(() -> listener.getSuccessObjects().size() == storedPayload.size());
        assertThat(listener.getSuccessObjects())
                .isEqualTo(storedPayload);
        consumer.close();
    }

    @Test
    public void testListenerOnFailedObjectsCalled() throws Exception {
        List<TestingPayload> storedObjects = generateTestingPayload(1, true);

        final AutomaticObjectQueueConsumer<TestingPayload> consumer = new AutomaticObjectQueueConsumer<>(
                queue,
                new AlwaysThrowsExceptionHandler<TestingPayload>() {
                    @Override
                    public HandleFailedStrategy onHandleObjectFailed(TestingPayload obj, Throwable throwable) {
                        return HandleFailedStrategy.SHUTDOWN;
                    }
                },
                listenerExecutor,
                Collections.singletonList(listener));

        await().until(() -> listener.getFailedObjects().size() == 1);
        await().until(consumer::closed);
        assertThat(listener.getFailedObjects())
                .isEqualTo(storedObjects);
        assertThat(listener.getFailedExceptions()).are(runtimeException);
        consumer.close();
    }

    private List<TestingPayload> generateTestingPayload(int size, boolean valid) {
        final List<TestingPayload> storedPayload = new ArrayList<>();
        for (int i = 1; i < size + 1; i++) {
            TestingPayload payload = new TestingPayload(numberStringBytes(i)).setValid(valid);
            storedPayload.add(payload);
        }

        storage.store(storedPayload);
        return storedPayload;
    }

    private static class AlwaysSuccessConsumeObjectHandler<E extends Verifiable> implements ConsumeObjectHandler<E> {
        private final List<E> receivedPayloads;

        AlwaysSuccessConsumeObjectHandler() {
            this.receivedPayloads = new ArrayList<>();
        }

        List<E> getReceivedObjects() {
            return receivedPayloads;
        }

        @Override
        public void onHandleObject(E obj) {
            receivedPayloads.add(obj);
        }


        @Override
        public HandleFailedStrategy onHandleObjectFailed(E obj, Throwable throwable) {
            throw new IllegalStateException("can't be here");
        }
    }

    private static abstract class AlwaysThrowsExceptionHandler<E extends Verifiable> extends AlwaysSuccessConsumeObjectHandler<E> {
        @Override
        public void onHandleObject(E obj) {
            throw new RuntimeException();
        }


        @Override
        public HandleFailedStrategy onHandleObjectFailed(E obj, Throwable throwable) {
            throw new RuntimeException();
        }
    }

    private static final class RetryAfterFailedHandler<E extends Verifiable> extends AlwaysSuccessConsumeObjectHandler<E> {
        private final int expectHandleTimes;
        private int handleTimes = 0;

        RetryAfterFailedHandler(int expectHandleTimes) {
            this.expectHandleTimes = expectHandleTimes;
        }

        int getHandleTimes() {
            return handleTimes;
        }

        @Override
        public void onHandleObject(E obj) {
            if (++handleTimes < expectHandleTimes) {
                throw new RuntimeException();
            }
        }


        @Override
        public HandleFailedStrategy onHandleObjectFailed(E obj, Throwable throwable) {
            return HandleFailedStrategy.RETRY;
        }
    }

    private static final class TestingConsumeObjectListener<E extends Verifiable> implements ConsumeObjectListener<E> {
        private final List<E> invalidObjects;
        private final List<E> successObjects;
        private final List<E> failedObjects;
        private final List<Throwable> failedExceptions;

        TestingConsumeObjectListener() {
            this.invalidObjects = new ArrayList<>();
            this.successObjects = new ArrayList<>();
            this.failedObjects = new ArrayList<>();
            this.failedExceptions = new ArrayList<>();
        }

        void clear() {
            invalidObjects.clear();
            successObjects.clear();
            failedObjects.clear();
            failedExceptions.clear();
        }

        List<E> getInvalidObjects() {
            return invalidObjects;
        }

        List<E> getSuccessObjects() {
            return successObjects;
        }

        List<E> getFailedObjects() {
            return failedObjects;
        }

        List<Throwable> getFailedExceptions() {
            return failedExceptions;
        }

        @Override
        public void onInvalidObject(E obj) {
            invalidObjects.add(obj);
        }

        @Override
        public void onHandleSuccess(E obj) {
            successObjects.add(obj);
        }

        @Override
        public void onHandleFailed(E obj, Throwable throwable) {
            failedObjects.add(obj);
            failedExceptions.add(throwable);
        }

        @Override
        public void onListenerNotificationFailed(Throwable throwable) {
            failedExceptions.add(throwable);
        }
    }
}