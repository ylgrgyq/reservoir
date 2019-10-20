package com.github.ylgrgyq.reservoir.benchmark.storage;

import com.github.ylgrgyq.reservoir.ObjectQueueStorage;
import com.github.ylgrgyq.reservoir.storage.FileStorageBuilder;

public final class FileStorageWriteBench extends StorageWriteBenchmark {
    FileStorageWriteBench(int dataSize, int numDataPerBatch, int numBatches) {
        super(dataSize, numDataPerBatch, numBatches);
    }

    @Override
    public String getTestDescription() {
        return "Write data to FileStorage test";
    }

    @Override
    ObjectQueueStorage<byte[]> createStorage(String baseDir) throws Exception {
        return FileStorageBuilder
                .newBuilder(baseDir)
                .build();
    }
}
