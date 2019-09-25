package com.github.ylgrgyq.reservoir;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * AutomaticObjectQueueConsumer is a wrapper over an {@link ObjectQueueConsumer}. It provide the ability
 * to consume objects from the wrapped {@link ObjectQueueConsumer} and put them to a provided
 * {@link ConsumeObjectHandler} to handle them automatically. You can also add {@link ConsumeObjectListener}
 * to this AutomaticObjectQueueConsumer to monitor the process of handling a consumed object. From the added
 * {@link ConsumeObjectListener}, you can know things such as if an object was handled, if an object was
 * ignored due to invalid or if an object was failed to handle.
 *
 * @param <E> the type of the object to consume
 */
public final class AutomaticObjectQueueConsumer<E extends Verifiable> implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(AutomaticObjectQueueConsumer.class);
    private static final ThreadFactory threadFactory = new NamedThreadFactory("automatic-object-queue-consumer-");

    private static final int UNINTERRUPTABLE = 0;
    private static final int INTERRUPTABLE = 1;
    private static final int SHUTDOWN = 2;

    private final ObjectQueueConsumer<E> delegateConsumer;
    private final Thread worker;
    @Nullable
    private final Executor listenerExecutor;
    private final Set<ConsumeObjectListener<E>> listeners;
    private final AtomicInteger workerState;
    private volatile boolean closed;

    /**
     * The strategy to use when handling a consumed object failed.
     */
    public enum HandleFailedStrategy {
        /**
         * Ignore this failed object. Commit and continue to consume the next object.
         */
        IGNORE,
        /**
         * Shutdown this consumer.
         */
        SHUTDOWN,

        /**
         * try to handle this failed object by {@link ConsumeObjectHandler#onHandleObject(Verifiable)} again.
         */
        RETRY,
    }

    /**
     * Handler for the consumed object.
     */
    public interface ConsumeObjectHandler<E extends Verifiable> {
        /**
         * Called when an object consumed from the Queue.
         *
         * @param obj the consumed object
         * @throws Exception any exception occurred while handling the consumed object
         */
        void onHandleObject(E obj) throws Exception;

        /**
         * Called when {@code onHandleObject(E)} throws an exception.
         *
         * @param obj       the consumed object who caused the exception
         * @param throwable the exception caught
         * @return strategy to handle this failed object
         */
        HandleFailedStrategy onHandleObjectFailed(E obj, Throwable throwable);
    }

    /**
     * Listener on consume object from the queue.
     */
    public interface ConsumeObjectListener<E extends Verifiable> {
        /**
         * Called when the consumed object is invalid. The object will be committed and dropped automatically.
         *
         * @param obj the consumed object
         */
        void onInvalidObject(E obj);

        /**
         * Called when handle the consumed object success.
         *
         * @param obj the consumed object
         */
        void onHandleSuccess(E obj);

        /**
         * Called when handle the consumed object failed.
         *
         * @param obj       the consumed object
         * @param throwable the exception thrown on handle the consumed object
         */
        void onHandleFailed(E obj, Throwable throwable);

        /**
         * Called when an exception was thrown when calling some listener.
         *
         * @param throwable the exception thrown
         */
        void onListenerNotificationFailed(Throwable throwable);
    }

    /**
     * Create a new instance without listener executor and {@link ConsumeObjectListener}s.
     * <p>
     * Please note this kind of consumer can not add {@link ConsumeObjectListener}s in the future
     * due to lack of listener executor.
     *
     * @param consumer the delegated consumer
     * @param handler  the handler for the consumed object from the delegated consumer
     */
    public AutomaticObjectQueueConsumer(ObjectQueueConsumer<E> consumer,
                                        ConsumeObjectHandler<E> handler) {
        this(consumer, handler, null, Collections.emptyList());
    }

    /**
     * Create a new instance without {@link ConsumeObjectListener}s.
     * It can add {@link ConsumeObjectListener} in the future.
     *
     * @param consumer         the delegated consumer
     * @param handler          the handler for the consumed object from the delegated consumer
     * @param listenerExecutor the executor used to call {@link ConsumeObjectListener}s, please
     *                         close it after this consumer is closed
     */
    public AutomaticObjectQueueConsumer(ObjectQueueConsumer<E> consumer,
                                        ConsumeObjectHandler<E> handler,
                                        @Nullable Executor listenerExecutor) {
        this(consumer, handler, listenerExecutor, Collections.emptyList());
    }

    /**
     * Create a new instance.
     *
     * @param consumer         the delegated consumer
     * @param handler          the handler for the consumed object from the delegated consumer
     * @param listenerExecutor the executor used to call listeners, please close it after
     *                         this consumer is closed
     * @param listeners        a list of {@link ConsumeObjectListener}s for this consumer
     */
    public AutomaticObjectQueueConsumer(ObjectQueueConsumer<E> consumer,
                                        ConsumeObjectHandler<E> handler,
                                        @Nullable Executor listenerExecutor,
                                        List<ConsumeObjectListener<E>> listeners) {
        requireNonNull(consumer, "consumer");
        requireNonNull(handler, "handler");
        requireNonNull(listeners, "listeners");

        if (listenerExecutor == null && !listeners.isEmpty()) {
            throw new IllegalArgumentException("listenerExecutor is null but listeners is not empty");
        }

        this.workerState = new AtomicInteger(UNINTERRUPTABLE);
        this.delegateConsumer = consumer;
        this.worker = threadFactory.newThread(new Worker(handler));
        this.listenerExecutor = listenerExecutor;
        this.listeners = new CopyOnWriteArraySet<>(listeners);
        this.worker.start();
    }

    /**
     * Add a new {@link ConsumeObjectListener} to this consumer.
     *
     * @param listener the new listener to add
     */
    public void addListener(ConsumeObjectListener<E> listener) {
        requireNonNull(listener, "listener");

        if (listenerExecutor == null) {
            throw new IllegalStateException("listenerExecutor is null");
        }

        listeners.add(listener);
    }

    /**
     * Remove a exists {@link ConsumeObjectListener} from this consumer.
     *
     * @param listener the target listener to remove
     * @return <tt>true</tt> if this consumer contained the specified listener
     */
    public boolean removeListener(ConsumeObjectListener<E> listener) {
        requireNonNull(listener, "listener");

        if (listenerExecutor == null) {
            throw new IllegalStateException("listenerExecutor is null");
        }

        return listeners.remove(listener);
    }

    @Override
    public void close() throws Exception {
        closed = true;

        if (worker != Thread.currentThread()) {
            while (workerState.get() != SHUTDOWN && !workerState.compareAndSet(INTERRUPTABLE, SHUTDOWN)) {
                Thread.sleep(100);
            }

            worker.interrupt();
            worker.join();
        } else {
            workerState.set(SHUTDOWN);
        }

        delegateConsumer.close();
    }

    boolean closed() {
        return closed;
    }

    private final class Worker implements Runnable {
        private final ConsumeObjectHandler<E> handler;

        Worker(ConsumeObjectHandler<E> handler) {
            this.handler = handler;
        }

        @Override
        public void run() {
            final ConsumeObjectHandler<E> handler = this.handler;
            final ObjectQueueConsumer<E> consumer = AutomaticObjectQueueConsumer.this.delegateConsumer;
            while (true) {
                try {
                    boolean commit = false;
                    try {
                        final E obj = fetchObject(consumer);

                        if (!obj.isValid()) {
                            notifyInvalidObject(obj);
                            commit = true;
                        } else {
                            commit = processFetchedObject(handler, obj);
                        }
                    } finally {
                        if (commit) {
                            consumer.commit();
                        }
                    }
                } catch (InterruptedException ex) {
                    if (closed) {
                        break;
                    }
                } catch (Exception ex) {
                    logger.warn("Got unexpected exception on processing object in reservoir queue.", ex);
                }
            }
        }
    }

    private E fetchObject(ObjectQueueConsumer<E> consumer) throws InterruptedException, StorageException {
        final boolean consumerMayClosed;
        final E obj;
        if (workerState.compareAndSet(UNINTERRUPTABLE, INTERRUPTABLE)) {
            try {
                obj = consumer.fetch();
                // If workerState set to SHUTDOWN state at here, the newly fetched obj may lost if
                // the underlying consumer set auto commit to true.
            } finally {
                consumerMayClosed = !workerState.compareAndSet(INTERRUPTABLE, UNINTERRUPTABLE);
            }
        } else {
            obj = null;
            consumerMayClosed = true;
        }

        if (consumerMayClosed) {
            throw new InterruptedException();
        }

        return obj;
    }

    private boolean processFetchedObject(ConsumeObjectHandler<E> handler, E obj) throws Exception {
        try {
            handler.onHandleObject(obj);
            notifyOnHandleSuccess(obj);
            return true;
        } catch (Exception ex) {
            notifyOnHandleFailed(obj, ex);
            return handleObjectFailed(handler, obj, ex);
        }
    }

    private boolean handleObjectFailed(ConsumeObjectHandler<E> handler, E obj, Throwable ex) throws Exception {
        boolean commit = false;
        HandleFailedStrategy strategy;
        try {
            strategy = handler.onHandleObjectFailed(obj, ex);
            if (strategy == null) {
                logger.error("ConsumeObjectHandler.onHandleObjectFailed returned null on handle" +
                                " object {}, and exception {}, shutdown anyway.",
                        obj, ex);
                strategy = HandleFailedStrategy.SHUTDOWN;
            }
        } catch (Exception ex2) {
            logger.error("Got unexpected exception from ConsumeObjectHandler.onHandleObjectFailed on handle " +
                    "object {}, and exception {}. Shutdown anyway.", obj, ex2);
            strategy = HandleFailedStrategy.SHUTDOWN;
        }

        switch (strategy) {
            case RETRY:
                break;
            case IGNORE:
                commit = true;
                break;
            case SHUTDOWN:
                close();
                break;
        }
        return commit;
    }

    private void notifyInvalidObject(E obj) {
        sendNotification(l -> l.onInvalidObject(obj));
    }

    private void notifyOnHandleSuccess(E obj) {
        sendNotification(l -> l.onHandleSuccess(obj));
    }

    private void notifyOnHandleFailed(E obj, Throwable ex) {
        sendNotification(l -> l.onHandleFailed(obj, ex));
    }

    private void sendNotification(Consumer<ConsumeObjectListener<E>> consumer) {
        if (listenerExecutor == null) {
            return;
        }

        try {
            listenerExecutor.execute(() -> {
                for (ConsumeObjectListener<E> l : listeners) {
                    try {
                        consumer.accept(l);
                    } catch (Exception ex) {
                        l.onListenerNotificationFailed(ex);
                    }
                }
            });
        } catch (Exception ex) {
            logger.error("Notification failed", ex);
        }
    }
}
