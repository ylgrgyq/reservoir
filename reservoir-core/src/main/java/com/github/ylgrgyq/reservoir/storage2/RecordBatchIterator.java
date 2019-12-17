package com.github.ylgrgyq.reservoir.storage2;

import java.io.IOException;

class RecordBatchIterator<T extends RecordBatch> extends AbstractIterator<T> {
    private final LogInputStream<T> logInputStream;

    RecordBatchIterator(LogInputStream<T> logInputStream) {
        this.logInputStream = logInputStream;
    }

    @Override
    protected T makeNext() {
        try {
            T batch = logInputStream.nextBatch();
            if (batch == null)
                return allDone();
            return batch;
        } catch (IOException e) {
            throw new StorageRuntimeException(e);
        }
    }
}
