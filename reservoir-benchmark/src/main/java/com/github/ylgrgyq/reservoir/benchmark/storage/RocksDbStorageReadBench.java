package com.github.ylgrgyq.reservoir.benchmark.storage;

import com.github.ylgrgyq.reservoir.ObjectQueueStorage;

public final class RocksDbStorageReadBench extends StorageReadBenchmark {
    RocksDbStorageReadBench(int dataSize, int readBatchSize, int numOfDataToRead, boolean randomReadData) {
        super(dataSize, readBatchSize, numOfDataToRead, randomReadData);
    }

    @Override
    ObjectQueueStorage<byte[]> createStorage(String baseDir) throws Exception {
        return new RocksDbStorage(baseDir);
    }

    @Override
    public String getTestDescription() {
        return "Read data from RocksDbStorage test";
    }
}
