package com.github.ylgrgyq.reservoir.benchmark;

import java.util.ArrayList;
import java.util.List;

import static com.github.ylgrgyq.reservoir.benchmark.TestingUtils.makeStringInBytes;

final public class TestingDataGenerator {
    public static List<List<byte[]>> generate(int dataSize, int numOfBatches, int numOfDataPerBatch) {
        final List<List<byte[]>> testingData = new ArrayList<>();

        for (int i = 0; i < numOfBatches; i++) {
            List<byte[]> batch = generateBatch(numOfDataPerBatch, dataSize);
            testingData.add(batch);
        }

        return testingData;
    }

    private static List<byte[]> generateBatch(int numOfDataPerBatch, int dataSize) {
        final List<byte[]> batch = new ArrayList<>();
        for (int i = 0; i < numOfDataPerBatch; i++) {
            batch.add(makeStringInBytes("Hello", dataSize));
        }
        return batch;
    }
}
