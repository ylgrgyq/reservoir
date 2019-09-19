package com.github.ylgrgyq.reservoir.benchmark.storage;

import java.util.ArrayList;
import java.util.List;

import static com.github.ylgrgyq.reservoir.benchmark.storage.TestingUtils.makeStringInBytes;

final class TestingDataGenerator {
    static List<List<byte[]>> generate(int dataSize, int dataCount, int batchSize) {
        final List<List<byte[]>> testingData = new ArrayList<>();

        for (int i = 0; i < dataCount; i++) {
            List<byte[]> batch = generateBatch(batchSize, dataSize);
            testingData.add(batch);
        }

        return testingData;
    }

    static List<byte[]> generateBatch(int batchSize, int dataSize) {
        final List<byte[]> batch = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            batch.add(makeStringInBytes("Hello", dataSize));
        }
        return batch;
    }
}
