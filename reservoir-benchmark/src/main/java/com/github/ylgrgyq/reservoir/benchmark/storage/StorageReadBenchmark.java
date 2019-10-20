package com.github.ylgrgyq.reservoir.benchmark.storage;

import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.github.ylgrgyq.reservoir.FileUtils;
import com.github.ylgrgyq.reservoir.ObjectQueueStorage;
import com.github.ylgrgyq.reservoir.SerializedObjectWithId;
import com.github.ylgrgyq.reservoir.StorageException;

import javax.annotation.Nullable;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

abstract class StorageReadBenchmark implements BenchmarkTest {
    private final int readBatchSize;
    private final int dataSize;
    private final int numOfDataToRead;
    private final List<List<byte[]>> testingData;
    private final String baseDir;
    @Nullable
    private ObjectQueueStorage<byte[]> storage;
    @Nullable
    private Timer timer;

    StorageReadBenchmark(int dataSize, int readBatchSize, int numOfDataToRead) {
        this.dataSize = dataSize;
        this.readBatchSize = readBatchSize;
        this.numOfDataToRead = numOfDataToRead;
        this.testingData = new ArrayList<>(TestingDataGenerator.generate(dataSize, numOfDataToRead, 2));

        final String tempDir = System.getProperty("java.io.tmpdir", "/tmp") +
                File.separator + "reservoir_benchmark_" + System.nanoTime();
        final File tempFile = new File(tempDir);
        this.baseDir = tempFile.getPath();
    }

    @Override
    public void setup() throws Exception {
        // Use new storage and timer every time to prevent interference between each test
        final String tempDir = baseDir + File.separator + "test_only_read" + System.nanoTime();
        final File tempFile = new File(tempDir);
        FileUtils.forceMkdir(tempFile);
        storage = createStorage(tempFile.getPath());
        timer = new Timer();

        for (List<byte[]> data : testingData) {
            storage.store(data);
        }
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
                "number of data per read: " + readBatchSize + "\n" +
                "total number of data to read: " + numOfDataToRead;
    }

    @Override
    public BenchmarkTestReport runTest() throws StorageException {
        assert timer != null;
        assert storage != null;
        final long start = System.nanoTime();
        long lastId = 0;
        int totalDataRead = 0;
        while (totalDataRead < numOfDataToRead) {
            final Context cxt = timer.time();
            try {
                List<SerializedObjectWithId<byte[]>> data = storage.fetch(lastId, readBatchSize);
                lastId = data.get(data.size() - 1).getId();
                totalDataRead += data.size();
            } catch (InterruptedException ex) {
                throw new StorageException("test was interrupted unexpectedly", ex);
            } finally {
                cxt.stop();
            }
        }

        return new DefaultBenchmarkReport(System.nanoTime() - start, timer);
    }

    abstract String getTestDescription();

    abstract ObjectQueueStorage<byte[]> createStorage(String baseDir) throws Exception;
}
