package com.github.ylgrgyq.reservoir;

import com.github.ylgrgyq.reservoir.AutomaticObjectQueueConsumer.ConsumeObjectListener;

public abstract class AbstractConsumeObjectListener<E extends Verifiable> implements ConsumeObjectListener<E> {
    @Override
    public void onInvalidObject(E obj) {

    }

    @Override
    public void onHandleSuccess(E obj) {

    }

    @Override
    public void onHandleFailed(E obj, Throwable throwable) {

    }

    @Override
    public void onListenerNotificationFailed(Throwable throwable) {

    }
}
