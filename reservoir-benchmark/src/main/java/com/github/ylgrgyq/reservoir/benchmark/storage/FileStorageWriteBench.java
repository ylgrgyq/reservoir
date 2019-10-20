package com.github.ylgrgyq.reservoir.benchmark.storage;

import com.github.ylgrgyq.reservoir.ObjectQueueStorage;
import com.github.ylgrgyq.reservoir.storage.FileStorageBuilder;

public final class FileStorageWriteBench extends StorageWriteBenchmark {
    private final boolean syncWriteWalLog;

    FileStorageWriteBench(int dataSize, int numOfDataPerBatch, int numOfBatches, boolean syncWriteWalLog) {
        super(dataSize, numOfDataPerBatch, numOfBatches);
        this.syncWriteWalLog = syncWriteWalLog;
    }

    @Override
    public String getTestDescription() {
        return "Write data to FileStorage test";
    }

    @Override
    ObjectQueueStorage<byte[]> createStorage(String baseDir) throws Exception {
        return FileStorageBuilder
                .newBuilder(baseDir)
                .syncWriteWalLog(syncWriteWalLog)
                .build();
    }
}
