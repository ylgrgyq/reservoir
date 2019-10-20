package com.github.ylgrgyq.reservoir.benchmark.storage;

import com.github.ylgrgyq.reservoir.ObjectQueueStorage;
import com.github.ylgrgyq.reservoir.storage.FileStorageBuilder;

public final class FileStorageReadBench extends StorageReadBenchmark {
    private final boolean syncWriteWalLog;

    FileStorageReadBench(int dataSize, int readBatchSize, int numOfDataToRead, boolean syncWriteWalLog, boolean randomReadData) {
        super(dataSize, readBatchSize, numOfDataToRead, randomReadData);
        this.syncWriteWalLog = syncWriteWalLog;
    }

    @Override
    public String getTestDescription() {
        return "Read data from FileStorage test.";
    }

    @Override
    ObjectQueueStorage<byte[]> createStorage(String baseDir) throws Exception {
        return FileStorageBuilder
                .newBuilder(baseDir)
                .syncFlushConsumerCommitLogWriter(syncWriteWalLog)
                .build();
    }
}
