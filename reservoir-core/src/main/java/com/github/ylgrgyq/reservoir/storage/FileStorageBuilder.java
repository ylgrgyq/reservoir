package com.github.ylgrgyq.reservoir.storage;

import com.github.ylgrgyq.reservoir.NamedThreadFactory;
import com.github.ylgrgyq.reservoir.StorageException;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * A builder to build {@link FileStorage}.
 */
public class FileStorageBuilder {
    /**
     * Create a new {@link FileStorageBuilder} instance.
     *
     * @param storageBaseDir a directory path on local file system to store files
     *                       for the building {@link FileStorage}. Please not that
     *                       every {@link FileStorage} instance should have their
     *                       own {@code storageBaseDir} and should not share it.
     * @return the new {@link FileStorageBuilder} instance
     */
    public static FileStorageBuilder newBuilder(String storageBaseDir) {
        requireNonNull(storageBaseDir, "storageBaseDir");

        return new FileStorageBuilder(storageBaseDir);
    }

    private boolean startWithCleanDirectory = false;
    private boolean forceSyncOnFlushConsumerCommitLogWriter = false;
    private boolean forceSyncOnFlushDataLogWriter = false;
    private long truncateIntervalMillis = TimeUnit.MINUTES.toMillis(1);
    private final String storageBaseDir;

    @Nullable
    private ExecutorService flushMemtableExecutorService;

    private FileStorageBuilder(final String storageBaseDir) {
        this.storageBaseDir = storageBaseDir;
    }

    /**
     * {@link FileStorage} will try to remove already committed objects from the storage periodically.
     * This method is used to set the checking interval.
     *
     * @param truncateInterval the interval to try to remove committed objects
     * @param unit             a {@code TimeUnit} determining how to interpret the
     *                         {@code truncateInterval} parameter
     * @return this
     */
    public FileStorageBuilder truncateIntervalMillis(long truncateInterval, TimeUnit unit) {
        if (truncateInterval < 0) {
            throw new IllegalArgumentException("truncateInterval: " + truncateInterval + " (expect: >= 0)");
        }
        requireNonNull(unit, "unit");

        truncateIntervalMillis = unit.toMillis(truncateInterval);
        return this;
    }

    /**
     * Set a {@link ExecutorService} instance used by {@link FileStorage} to flush internal memtable into file.
     * If not set, {@link FileStorageBuilder} will create a single thread {@link ExecutorService}
     * for the building {@link FileStorage}.
     * {@link FileStorage} will shutdown this {@link ExecutorService} when it is closed.
     *
     * @param flushMemtableExecutorService a {@link ExecutorService} instance
     * @return this
     */
    public FileStorageBuilder flushMemtableExecutorService(ExecutorService flushMemtableExecutorService) {
        requireNonNull(flushMemtableExecutorService, "flushMemtableExecutorService");

        this.flushMemtableExecutorService = flushMemtableExecutorService;
        return this;
    }

    /**
     * After {@link FileStorage#commitId(long)}, forces updates on any internal file to be written to
     * the underlying storage device. This may cause a huge degradation on performance. But will reduce
     * the probability of fetching an object from {@link FileStorage} more than once.
     * <p>
     * The default value is false.
     *
     * @param syncFlushConsumerCommitLogWriter true to force flush updated commit id to internal file
     * @return this
     */
    public FileStorageBuilder syncFlushConsumerCommitLogWriter(boolean syncFlushConsumerCommitLogWriter) {
        this.forceSyncOnFlushConsumerCommitLogWriter = syncFlushConsumerCommitLogWriter;
        return this;
    }

    /**
     * After {@link FileStorage#store(List)}, forces updates on internal WAL log file to be written to
     * the underlying storage device. This may cause a huge degradation on write performance.
     * But will reduce the probability of losing data after {@link FileStorage#store(List)} success.
     * <p>
     * The default value is false.
     *
     * @param syncWriteWalLog true to force flush stored data to internal WAL log file
     * @return this
     */
    public FileStorageBuilder syncWriteWalLog(boolean syncWriteWalLog) {
        this.forceSyncOnFlushDataLogWriter = syncWriteWalLog;
        return this;
    }

    /**
     * If set, when {@link FileStorage} is created, it will scan every existing files in it's working directory,
     * delete any files which name matches the name pattern used in {@link FileStorage}. In this way, it
     * will delete all the files saved by previous {@link FileStorage}s using the same working directory and
     * working with a clean directory. Thus the newly created {@link FileStorage} can not recover any data
     * saved before.
     * <p>
     * The default value is false.
     *
     * @param startWithCleanDirectory true to delete all the files saved by previous {@link FileStorage}s
     *                                using the same working directory
     * @return this
     */
    public FileStorageBuilder startWithCleanDirectory(boolean startWithCleanDirectory) {

        this.startWithCleanDirectory = startWithCleanDirectory;
        return this;
    }

    /**
     * Create a new instance of {@link FileStorage}.
     *
     * @return a new instance of {@link FileStorage}.
     * @throws StorageException if any error happens in underlying storage
     */
    public FileStorage build() throws StorageException {
        if (flushMemtableExecutorService == null) {
            flushMemtableExecutorService = Executors.newSingleThreadScheduledExecutor(
                    new NamedThreadFactory("memtable-writer-"));
        }
        return new FileStorage(this);
    }

    boolean startWithCleanDirectory() {
        return startWithCleanDirectory;
    }

    String getStorageBaseDir() {
        return storageBaseDir;
    }

    long getTruncateIntervalMillis() {
        return truncateIntervalMillis;
    }

    ExecutorService getFlushMemtableExecutorService() {
        if (flushMemtableExecutorService == null) {
            flushMemtableExecutorService = Executors.newSingleThreadScheduledExecutor(
                    new NamedThreadFactory("memtable-writer-"));
        }
        return flushMemtableExecutorService;
    }

    boolean forceSyncOnFlushConsumerCommitLogWriter() {
        return forceSyncOnFlushConsumerCommitLogWriter;
    }

    boolean forceSyncOnFlushDataLogWriter() {
        return forceSyncOnFlushDataLogWriter;
    }
}
