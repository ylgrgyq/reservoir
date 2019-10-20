package com.github.ylgrgyq.reservoir.benchmark.storage;

import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.github.ylgrgyq.reservoir.FileUtils;
import com.github.ylgrgyq.reservoir.ObjectQueueStorage;
import com.github.ylgrgyq.reservoir.StorageException;
import com.github.ylgrgyq.reservoir.benchmark.BenchmarkTest;
import com.github.ylgrgyq.reservoir.benchmark.BenchmarkTestReport;
import com.github.ylgrgyq.reservoir.benchmark.DefaultBenchmarkReport;
import com.github.ylgrgyq.reservoir.benchmark.TestingDataGenerator;

import javax.annotation.Nullable;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

abstract class StorageWriteBenchmark implements BenchmarkTest {
    private final int numOfBatches;
    private final int dataSize;
    private final int numOfDataPerBatch;
    private final List<List<byte[]>> testingData;
    private final String baseDir;
    @Nullable
    private ObjectQueueStorage<byte[]> storage;
    @Nullable
    private Timer timer;

    StorageWriteBenchmark(int dataSize, int numOfDataPerBatch, int numOfBatches) {
        this.dataSize = dataSize;
        this.numOfBatches = numOfBatches;
        this.numOfDataPerBatch = numOfDataPerBatch;
        this.testingData = new ArrayList<>(TestingDataGenerator.generate(dataSize, numOfBatches, numOfDataPerBatch));

        final String tempDir = System.getProperty("java.io.tmpdir", "/tmp") +
                File.separator + "reservoir_benchmark_" + System.nanoTime();
        final File tempFile = new File(tempDir);
        this.baseDir = tempFile.getPath();
    }

    @Override
    public void setup() throws Exception {
        // Use new storage and timer every time to prevent interference between each test
        final String tempDir = baseDir + File.separator + "test_only_write" + System.nanoTime();
        final File tempFile = new File(tempDir);
        FileUtils.forceMkdir(tempFile);
        storage = createStorage(tempFile.getPath());
        timer = new Timer();
    }

    @Override
    public void teardown() throws Exception {
        assert storage != null;
        storage.close();
    }

    @Override
    public String testingSpec() {
        return "description: " + getTestDescription() + "\n" +
                "storage path: " + baseDir + "\n" +
                "size in bytes for each data: " + dataSize + "\n" +
                "number of data per batch: " + numOfDataPerBatch + "\n" +
                "number of batches: " + numOfBatches;
    }

    @Override
    public BenchmarkTestReport runTest() throws StorageException {
        assert timer != null;
        assert storage != null;
        final long start = System.nanoTime();
        for (List<byte[]> data : testingData) {
            final Context cxt = timer.time();
            try {
                storage.store(data);
            } finally {
                cxt.stop();
            }
        }

        return new DefaultBenchmarkReport(System.nanoTime() - start, timer);
    }

    abstract String getTestDescription();

    abstract ObjectQueueStorage<byte[]> createStorage(String baseDir) throws Exception;
}
