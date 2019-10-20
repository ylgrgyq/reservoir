package com.github.ylgrgyq.reservoir.benchmark.storage;

import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.github.ylgrgyq.reservoir.FileUtils;
import com.github.ylgrgyq.reservoir.ObjectQueueStorage;
import com.github.ylgrgyq.reservoir.SerializedObjectWithId;
import com.github.ylgrgyq.reservoir.StorageException;
import com.github.ylgrgyq.reservoir.benchmark.BenchmarkTest;
import com.github.ylgrgyq.reservoir.benchmark.BenchmarkTestReport;
import com.github.ylgrgyq.reservoir.benchmark.DefaultBenchmarkReport;
import com.github.ylgrgyq.reservoir.benchmark.TestingDataGenerator;

import javax.annotation.Nullable;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

abstract class StorageReadBenchmark implements BenchmarkTest {
    // We prepare twice the size of numOfDataToRead data in storage to have
    // a larger range for testing random read.
    private static final int DEFAULT_NUM_OF_DATA_PER_BATCH = 2;

    private final int readBatchSize;
    private final int dataSize;
    private final int numOfDataToRead;
    private final List<List<byte[]>> testingData;
    private final String baseDir;
    private final List<Long> readFromIds;
    @Nullable
    private ObjectQueueStorage<byte[]> storage;
    @Nullable
    private Timer timer;

    StorageReadBenchmark(int dataSize, int readBatchSize, int numOfDataToRead, boolean randomReadData) {
        this.dataSize = dataSize;
        this.readBatchSize = readBatchSize;
        this.numOfDataToRead = numOfDataToRead;
        this.testingData = new ArrayList<>(TestingDataGenerator.generate(dataSize, numOfDataToRead, DEFAULT_NUM_OF_DATA_PER_BATCH));

        final String tempDir = System.getProperty("java.io.tmpdir", "/tmp") +
                File.separator + "reservoir_benchmark_" + System.nanoTime();
        final File tempFile = new File(tempDir);
        this.baseDir = tempFile.getPath();

        final int expectReadTimes = numOfDataToRead / readBatchSize;
        readFromIds = new ArrayList<>(expectReadTimes);
        if (randomReadData) {
            final long maxFromId = DEFAULT_NUM_OF_DATA_PER_BATCH * numOfDataToRead - readBatchSize - 1;

            for (int i = 0; i < expectReadTimes; i++) {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                readFromIds.add(random.nextLong(maxFromId));
            }
        } else {
            for (long i = 0; i < numOfDataToRead; i += readBatchSize) {
                readFromIds.add(i);
            }
        }
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
        int totalDataRead = 0;
        for (Long id : readFromIds) {
            final Context cxt = timer.time();
            try {
                List<SerializedObjectWithId<byte[]>> data = storage.fetch(id, readBatchSize);
                totalDataRead += data.size();
            } catch (InterruptedException ex) {
                throw new StorageException("test was interrupted unexpectedly", ex);
            } finally {
                cxt.stop();
            }
        }

        if (totalDataRead != numOfDataToRead) {
            throw new RuntimeException(String.format("Testing invariant failed. " +
                    "expectNumOfDataToRead: %s, actual: %s", numOfDataToRead, totalDataRead));
        }

        return new DefaultBenchmarkReport(System.nanoTime() - start, timer);
    }

    abstract String getTestDescription();

    abstract ObjectQueueStorage<byte[]> createStorage(String baseDir) throws Exception;
}
