package com.github.ylgrgyq.reservoir;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

abstract class AbstractTestingStorage<S> implements ObjectQueueStorage<S> {
    @Override
    public void commitId(long id) throws StorageException {

    }

    @Override
    public long getLastCommittedId() throws StorageException {
        return 0;
    }

    @Override
    public List<SerializedObjectWithId<S>> fetch(long fromId, int limit) throws InterruptedException, StorageException {
        return Collections.emptyList();
    }

    @Override
    public List<SerializedObjectWithId<S>> fetch(long fromId, int limit, long timeout, TimeUnit unit) throws InterruptedException, StorageException {
        return Collections.emptyList();
    }

    @Override
    public void store(List<S> batch) throws StorageException {

    }

    @Override
    public void close() throws Exception {

    }
}
