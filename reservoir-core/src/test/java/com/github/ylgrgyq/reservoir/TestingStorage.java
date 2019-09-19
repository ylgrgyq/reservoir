package com.github.ylgrgyq.reservoir;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TestingStorage<S> extends AbstractTestingStorage<S>{
    private final ArrayList<SerializedObjectWithId<S>> producedPayloads;
    private long lastProducedId;
    private long lastCommittedId;
    private boolean closed;

    public TestingStorage() {
        this.producedPayloads = new ArrayList<>();
        this.lastProducedId = 0;
        this.lastCommittedId = -1;
    }

    synchronized List<SerializedObjectWithId<S>> getProdcedPayloads() {
        return new ArrayList<>(producedPayloads);
    }

    @Override
    public synchronized void commitId(long id) {
        lastCommittedId = id;
    }

    @Override
    public synchronized long getLastCommittedId() {
        return lastCommittedId;
    }

    @Override
    public synchronized List<SerializedObjectWithId<S>> fetch(long fromId, int limit) throws InterruptedException {
        ArrayList<SerializedObjectWithId<S>> ret = new ArrayList<>();
        while (true) {
            while (producedPayloads.isEmpty()) {
                wait();
            }

            SerializedObjectWithId<S> firstPayload = producedPayloads.get(0);
            int start = (int) (fromId - firstPayload.getId()) + 1;
            start = Math.max(0, start);
            int end = Math.min(start + limit, producedPayloads.size());

            if (start < end) {
                ret.addAll(producedPayloads.subList(start, end));
            }

            if (ret.isEmpty()) {
                wait();
                continue;
            }

            return ret;
        }
    }

    @Override
    public synchronized List<SerializedObjectWithId<S>> fetch(long fromId, int limit, long timeout, TimeUnit unit) throws InterruptedException {
        final long endNanos = System.nanoTime() + unit.toNanos(timeout);
        final ArrayList<SerializedObjectWithId<S>> ret = new ArrayList<>();
        while (true) {
            while (producedPayloads.isEmpty()) {
                long millisRemain = TimeUnit.NANOSECONDS.toMillis(endNanos - System.nanoTime());
                if (millisRemain <= 0) {
                    return ret;
                }

                wait(millisRemain);
            }

            SerializedObjectWithId<S> firstPayload = producedPayloads.get(0);
            int start = (int) (fromId - firstPayload.getId()) + 1;
            start = Math.max(0, start);
            int end = Math.min(start + limit, producedPayloads.size());

            if (start < end) {
                ret.addAll(producedPayloads.subList(start, end));
            }

            if (ret.isEmpty()) {
                long millisRemain = TimeUnit.NANOSECONDS.toMillis(endNanos - System.nanoTime());
                if (millisRemain <= 0) {
                    return ret;
                }

                wait(millisRemain);
                continue;
            }

            return ret;
        }
    }

    @Override
    public synchronized void store(List<S> batch) {
        long id = lastProducedId;
        for (S data : batch) {
            doAdd(new SerializedObjectWithId<S>(++id, data));
        }
        notify();
    }

    @Override
    public void close() throws Exception {
        closed = true;
    }

    boolean closed() {
        return closed;
    }

    synchronized void clear() {
        producedPayloads.clear();
        lastProducedId = 0;
        lastCommittedId = -1;
    }

    private void doAdd(SerializedObjectWithId<S> obj) {
        producedPayloads.add(obj);
        assert lastProducedId != obj.getId() :
                "lastProducedId: " + lastProducedId + " payloadId:" + obj.getId();
        if (obj.getId() > lastProducedId) {
            lastProducedId = obj.getId();
        }
    }
}
