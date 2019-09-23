package com.github.ylgrgyq.reservoir.benchmark.storage;

import com.github.ylgrgyq.reservoir.NamedThreadFactory;
import com.github.ylgrgyq.reservoir.ObjectQueueStorage;
import com.github.ylgrgyq.reservoir.SerializedObjectWithId;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public final class RocksDbStorage implements ObjectQueueStorage<byte[]> {
    private static final Logger logger = LoggerFactory.getLogger(RocksDbStorage.class);
    private static final String DEFAULT_QUEUE_NAME = "reservoir_queue";
    private static final byte[] CONSUMER_COMMIT_ID_META_KEY = "consumer_committed_id".getBytes(StandardCharsets.UTF_8);
    private static final ThreadFactory threadFactory = new NamedThreadFactory("storage-background-truncate-handler-");

    private final Thread backgroundTruncateHandler;

    private final List<ColumnFamilyOptions> cfOptions;
    private final long readRetryIntervalMillis;

    private final RocksDB db;
    private final ColumnFamilyHandle defaultColumnFamilyHandle;
    private final ColumnFamilyHandle columnFamilyHandle;
    private final List<ColumnFamilyHandle> columnFamilyHandles;
    private final WriteOptions writeOptions;
    private final ReadOptions readOptions;
    private final DBOptions dbOptions;

    private volatile boolean closed;

    static {
        RocksDB.loadLibrary();
    }

    public RocksDbStorage(String path) throws InterruptedException {
        this(path, false, 500, TimeUnit.MINUTES.toMillis(1));
    }

    public RocksDbStorage(String path, boolean destroyPreviousDbFiles) throws InterruptedException {
        this(path, destroyPreviousDbFiles, 500, TimeUnit.MINUTES.toMillis(1));
    }

    public RocksDbStorage(String path, boolean destroyPreviousDbFiles, long readRetryIntervalMillis) throws InterruptedException {
        this(path, destroyPreviousDbFiles, readRetryIntervalMillis, TimeUnit.MINUTES.toMillis(1));
    }

    public RocksDbStorage(String path, boolean destroyPreviousDbFiles, long readRetryIntervalMillis, long detectTruncateIntervalMillis) throws InterruptedException {
        this.cfOptions = new ArrayList<>();
        this.readRetryIntervalMillis = readRetryIntervalMillis;

        try {
            final DBOptions dbOptions = createDefaultRocksDBOptions();
            dbOptions.setCreateMissingColumnFamilies(true);
            dbOptions.setCreateIfMissing(true);
            this.dbOptions = dbOptions;

            final WriteOptions writeOptions = new WriteOptions();
            writeOptions.setSync(false);
            this.writeOptions = writeOptions;

            final ReadOptions totalOrderReadOptions = new ReadOptions();
            totalOrderReadOptions.setTotalOrderSeek(true);
            this.readOptions = totalOrderReadOptions;

            final File dir = new File(path);
            if (dir.exists() && !dir.isDirectory()) {
                throw new IllegalStateException("Invalid log path, it's a regular file: " + path);
            }

            final ColumnFamilyOptions columnFamilyOptions = createDefaultColumnFamilyOptions();
            if (destroyPreviousDbFiles) {
                try (Options destroyOptions = new Options(dbOptions, columnFamilyOptions)) {
                    RocksDB.destroyDB(path, destroyOptions);
                }
            }

            final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions));
            ColumnFamilyOptions options = createDefaultColumnFamilyOptions();
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(DEFAULT_QUEUE_NAME.getBytes(StandardCharsets.UTF_8), options));

            this.columnFamilyHandles = new ArrayList<>();
            this.db = RocksDB.open(dbOptions, path, columnFamilyDescriptors, columnFamilyHandles);
            this.defaultColumnFamilyHandle = columnFamilyHandles.get(0);
            this.columnFamilyHandle = columnFamilyHandles.get(1);

            this.backgroundTruncateHandler = threadFactory.newThread(new BackgroundTruncateHandler(detectTruncateIntervalMillis));
            this.backgroundTruncateHandler.start();
        } catch (final RocksDBException ex) {
            String msg = String.format("init RocksDb on path %s failed", path);
            close();
            throw new IllegalStateException(msg, ex);
        }
    }

    @Override
    public void commitId(long id) {
        try {
            final byte[] bs = new byte[8];
            Bits.putLong(bs, 0, id);
            db.put(defaultColumnFamilyHandle, writeOptions, CONSUMER_COMMIT_ID_META_KEY, bs);
        } catch (RocksDBException ex) {
            throw new IllegalStateException("fail to commit id: " + id, ex);
        }
    }

    @Override
    public long getLastCommittedId() {
        try {
            final byte[] commitIdInBytes = db.get(defaultColumnFamilyHandle, readOptions, CONSUMER_COMMIT_ID_META_KEY);
            if (commitIdInBytes != null) {
                return Bits.getLong(commitIdInBytes, 0);
            } else {
                return 0;
            }
        } catch (RocksDBException ex) {
            throw new IllegalStateException("fail to get last committed id: ", ex);
        }
    }

    long getLastProducedId() {
        try (final RocksIterator it = db.newIterator(columnFamilyHandle, readOptions)) {
            it.seekToLast();
            if (it.isValid()) {
                return Bits.getLong(it.key(), 0);
            }
            return 0;
        }
    }

    @Override
    public List<SerializedObjectWithId<byte[]>> fetch(final long fromId, final int limit) throws InterruptedException {
        if (fromId < 0) {
            throw new IllegalArgumentException("fromId: " + fromId + " (expect: >=0)");
        }

        final List<SerializedObjectWithId<byte[]>> entries = new ArrayList<>(limit);
        while (true) {
            try (RocksIterator it = db.newIterator(columnFamilyHandle, readOptions)) {
                for (it.seek(getKeyBytes(fromId)); it.isValid() && entries.size() < limit; it.next()) {
                    final long id = Bits.getLong(it.key(), 0);
                    if (id == fromId) {
                        continue;
                    }
                    final SerializedObjectWithId<byte[]> entry = new SerializedObjectWithId<>(id, it.value());
                    entries.add(entry);
                }
            }

            if (entries.isEmpty()) {
                Thread.sleep(readRetryIntervalMillis);
            } else {
                break;
            }
        }

        return Collections.unmodifiableList(entries);
    }

    @Override
    public List<SerializedObjectWithId<byte[]>> fetch(final long fromId, final int limit, final long timeout, final TimeUnit unit)
            throws InterruptedException {
        if (fromId < 0) {
            throw new IllegalArgumentException("fromId: " + fromId + " (expect: >=0)");
        }

        final long end = System.nanoTime() + unit.toNanos(timeout);
        final List<SerializedObjectWithId<byte[]>> entries = new ArrayList<>(limit);
        while (true) {
            try (RocksIterator it = db.newIterator(columnFamilyHandle, readOptions)) {
                for (it.seek(getKeyBytes(fromId)); it.isValid() && entries.size() < limit; it.next()) {
                    final long id = Bits.getLong(it.key(), 0);
                    if (id == fromId) {
                        continue;
                    }
                    final SerializedObjectWithId<byte[]> entry = new SerializedObjectWithId<>(id, it.value());
                    entries.add(entry);
                }
            }

            if (!entries.isEmpty()) {
                break;
            }

            final long remain = TimeUnit.NANOSECONDS.toMillis(end - System.nanoTime());
            if (remain <= 0) {
                break;
            }

            Thread.sleep(Math.min(remain, readRetryIntervalMillis));
        }

        return Collections.unmodifiableList(entries);
    }

    @Override
    public void store(List<byte[]> queue) {
        requireNonNull(queue, "queue");

        try (final WriteBatch batch = new WriteBatch()){
            long id = getLastProducedId();
            for (byte[] e : queue) {
                batch.put(columnFamilyHandle, getKeyBytes(++id), e);
            }
            db.write(writeOptions, batch);
        } catch (final RocksDBException e) {
            throw new IllegalStateException("fail to append entry", e);
        }
    }

    @Override
    public void close() throws InterruptedException {
        closed = true;

        if (backgroundTruncateHandler != null) {
            backgroundTruncateHandler.interrupt();
            backgroundTruncateHandler.join();
        }

        // The shutdown order is matter.
        // 1. close db and column family handles
        closeDB();
        // 2. close internal options.
        closeOptions();
    }

    private DBOptions createDefaultRocksDBOptions() {
        // Turn based on https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
        final DBOptions opts = new DBOptions();

        // If this value is set to true, then the database will be created if it is
        // missing during {@code RocksDB.open()}.
        opts.setCreateIfMissing(true);

        // If true, missing column families will be automatically created.
        opts.setCreateMissingColumnFamilies(true);

        // Number of open files that can be used by the DB.  You may need to increase
        // this if your database has a large working set. Value -1 means files opened
        // are always kept open.
        opts.setMaxOpenFiles(-1);

        // The maximum number of concurrent background compactions. The default is 1,
        // but to fully utilize your CPU and storage you might want to increase this
        // to approximately number of cores in the system.
        int cpus = Runtime.getRuntime().availableProcessors();
        opts.setMaxBackgroundCompactions(Math.min(cpus, 4));

        // The maximum number of concurrent flush operations. It is usually good enough
        // to set this to 1.
        opts.setMaxBackgroundFlushes(1);

        return opts;
    }

    private ColumnFamilyOptions createDefaultColumnFamilyOptions() {
//        final BlockBasedTableConfig tableConfig = new BlockBasedTableConfig(). //
//                setIndexType(IndexType.kHashSearch). // use hash search(btree) for prefix scan.
//                setBlockSize(4 * SizeUnit.KB).//
//                setFilterPolicy(new BloomFilter(16, false)).
//                setCacheIndexAndFilterBlocks(true). //
//                setBlockCache(new LRUCache(512 * SizeUnit.MB, 8));

        final ColumnFamilyOptions options = new ColumnFamilyOptions();
//        options.setTableFormatConfig(tableConfig);
//        options.useCappedPrefixExtractor()
        cfOptions.add(options);

        return options;
    }

    private void closeDB() {
        for (ColumnFamilyHandle handle : columnFamilyHandles) {
            handle.close();
        }

        if (db != null) {
            db.close();
        }
    }

    private void closeOptions() {
        // 1. close db options
        if(dbOptions!= null) {
            dbOptions.close();
        }

        // 2. close column family options.
        if (cfOptions != null) {
            for (final ColumnFamilyOptions opt : cfOptions) {
                opt.close();
            }
            cfOptions.clear();
        }

        // 3. close write/fetch options
        if (writeOptions != null) {
            writeOptions.close();
        }
        if (readOptions != null) {
            readOptions.close();
        }
    }


    private byte[] getKeyBytes(long id) {
        final byte[] ks = new byte[8];
        Bits.putLong(ks, 0, id);
        return ks;
    }

    private class BackgroundTruncateHandler implements Runnable {
        private final long detectTruncateIntervalMillis;

        BackgroundTruncateHandler(long detectTruncateIntervalMillis) {
            this.detectTruncateIntervalMillis = detectTruncateIntervalMillis;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    long lastCommittedId = getLastCommittedId();
                    long truncateId = Math.max(0, lastCommittedId - 1000);
                    try {
                        db.deleteRange(columnFamilyHandle, getKeyBytes(0), getKeyBytes(truncateId));
                    } catch (final RocksDBException e) {
                        logger.error("Fail to truncatePrefix {}", truncateId, e);
                    }

                    Thread.sleep(detectTruncateIntervalMillis);
                } catch (InterruptedException ex) {
                    if (closed){
                        break;
                    }
                } catch (Exception ex) {
                    logger.error("Truncate handler failed for entry", ex);
                    break;
                }
            }
        }
    }
}
