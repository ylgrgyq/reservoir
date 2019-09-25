package com.github.ylgrgyq.reservoir.storage;

import com.github.ylgrgyq.reservoir.*;
import com.github.ylgrgyq.reservoir.storage.FileName.FileNameMeta;
import com.github.ylgrgyq.reservoir.storage.FileName.FileType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * An {@link ObjectQueueStorage} implementation based on local file system.
 * <p>
 * Provide at least once commitment on {@link #commitId(long)}. So after a recovery an
 * already committed object may be fetched again.
 */
public final class FileStorage implements ObjectQueueStorage<byte[]> {
    private static final Logger logger = LoggerFactory.getLogger(FileStorage.class.getName());
    private static final ThreadFactory safeCloseThreadFactory = new NamedThreadFactory("safe-close-thread-");

    private static class Itr implements Iterator<SerializedObjectWithId<byte[]>> {
        private final List<SeekableIterator<Long, SerializedObjectWithId<byte[]>>> iterators;
        private int lastItrIndex;

        Itr(List<SeekableIterator<Long, SerializedObjectWithId<byte[]>>> iterators) {
            this.iterators = iterators;
        }

        @Override
        public boolean hasNext() {
            for (int i = lastItrIndex; i < iterators.size(); i++) {
                SeekableIterator<Long, SerializedObjectWithId<byte[]>> itr = iterators.get(i);
                if (itr.hasNext()) {
                    lastItrIndex = i;
                    return true;
                }
            }

            lastItrIndex = iterators.size();
            return false;
        }

        @Override
        public SerializedObjectWithId<byte[]> next() {
            assert lastItrIndex >= 0;

            if (lastItrIndex >= iterators.size()) {
                throw new NoSuchElementException();
            }

            return iterators.get(lastItrIndex).next();
        }
    }

    private final ExecutorService sstableWriterPool;
    private final String baseDir;
    private final TableCache tableCache;
    private final Manifest manifest;
    private final FileLock storageLock;
    private final boolean forceSyncOnFlushConsumerCommitLogWriter;
    private final boolean forceSyncOnFlushDataLogWriter;
    private final Lock lock;
    private final Condition storageNotEmpty;
    private final Condition noPendingImmFlushing;

    @Nullable
    private LogWriter dataLogWriter;
    private volatile int dataLogFileNumber;
    @Nullable
    private LogWriter consumerCommitLogWriter;
    private volatile int consumerCommitLogFileNumber;
    private long lastCommittedId;
    private long lastTryTruncateTime;
    private long truncateIntervalNanos;
    private Memtable mm;
    @Nullable
    private Memtable imm;
    @Nullable
    private volatile CompletableFuture<Void> closeFuture;
    @Nullable
    private Thread safeCloseThread;

    FileStorage(FileStorageBuilder builder) throws StorageException {
        requireNonNull(builder, "builder");
        final String storageBaseDir = builder.getStorageBaseDir();
        Path baseDirPath = Paths.get(storageBaseDir);

        if (Files.exists(baseDirPath) && !Files.isDirectory(baseDirPath)) {
            throw new IllegalArgumentException("\"" + storageBaseDir + "\" must be a directory");
        }

        this.sstableWriterPool = builder.getFlushMemtableExecutorService();
        this.mm = new Memtable();
        this.baseDir = storageBaseDir;
        this.lastCommittedId = Long.MIN_VALUE;
        this.tableCache = new TableCache(baseDir);
        this.manifest = new Manifest(baseDir);
        this.truncateIntervalNanos = TimeUnit.MILLISECONDS.toNanos(builder.getTruncateIntervalMillis());
        this.forceSyncOnFlushConsumerCommitLogWriter = builder.forceSyncOnFlushConsumerCommitLogWriter();
        this.forceSyncOnFlushDataLogWriter = builder.forceSyncOnFlushDataLogWriter();
        this.closeFuture = null;
        this.lock = new ReentrantLock();
        this.storageNotEmpty = lock.newCondition();
        this.noPendingImmFlushing = lock.newCondition();

        boolean initStorageSuccess = false;
        try {
            createStorageDir();

            this.storageLock = lockStorage(storageBaseDir);

            logger.debug("Start init storage under {}", storageBaseDir);

            if (builder.startWithCleanDirectory()) {
                cleanWorkingDirectory(storageBaseDir);
            }

            final ManifestRecord record = ManifestRecord.newPlainRecord();
            final Path currentFilePath = Paths.get(storageBaseDir, FileName.getCurrentFileName());
            if (Files.exists(currentFilePath)) {
                recoverStorage(currentFilePath, record);
            }

            if (this.dataLogWriter == null) {
                this.dataLogWriter = createNewDataLogWriter();
            }

            if (this.consumerCommitLogWriter == null) {
                this.consumerCommitLogWriter = createConsumerCommitLogWriter();
            }

            record.setDataLogFileNumber(this.dataLogFileNumber);
            record.setConsumerCommitLogFileNumber(this.consumerCommitLogFileNumber);
            this.manifest.logRecord(record);

            this.lastTryTruncateTime = System.nanoTime();
            initStorageSuccess = true;
        } catch (IOException | StorageException t) {
            throw new IllegalStateException("init storage failed", t);
        } catch (Exception ex) {
            throw new StorageException(ex);
        } finally {
            if (!initStorageSuccess) {
                blockSafeClose();
            }
        }
    }

    @Override
    public void commitId(long id) throws StorageException {
        if (closed()) {
            throw new IllegalStateException("storage is closed");
        }

        lock.lock();
        try {
            if (id > lastCommittedId) {
                assert consumerCommitLogWriter != null;
                final byte[] bs = new byte[8];
                Bits.putLong(bs, 0, id);
                consumerCommitLogWriter.append(bs);
                consumerCommitLogWriter.flush(forceSyncOnFlushConsumerCommitLogWriter);
                lastCommittedId = id;

                if (System.nanoTime() - lastTryTruncateTime > truncateIntervalNanos) {
                    tryTruncate();
                }
            }
        } catch (IOException ex) {
            throw new StorageException(ex);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long getLastCommittedId() {
        lock.lock();
        try {
            return lastCommittedId;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<SerializedObjectWithId<byte[]>> fetch(long fromId, int limit) throws InterruptedException, StorageException {
        if (closed()) {
            throw new IllegalStateException("storage is closed");
        }

        List<SerializedObjectWithId<byte[]>> entries;
        lock.lockInterruptibly();
        try {
            while (true) {
                entries = doFetch(fromId, limit);

                if (!entries.isEmpty()) {
                    break;
                }

                storageNotEmpty.await();
            }
        } finally {
            lock.unlock();
        }

        return Collections.unmodifiableList(entries);
    }

    @Override
    public List<SerializedObjectWithId<byte[]>> fetch(long fromId, int limit, long timeout, TimeUnit unit) throws InterruptedException, StorageException {
        if (closed()) {
            throw new IllegalStateException("storage is closed");
        }

        final long end = System.nanoTime() + unit.toNanos(timeout);
        List<SerializedObjectWithId<byte[]>> entries;
        lock.lockInterruptibly();
        try {
            while (true) {
                entries = doFetch(fromId, limit);

                if (!entries.isEmpty()) {
                    break;
                }

                final long remain = TimeUnit.NANOSECONDS.toMillis(end - System.nanoTime());
                if (remain <= 0) {
                    break;
                }

                if (storageNotEmpty.await(remain, TimeUnit.MILLISECONDS)) {
                    // we don't need to fetch storage again because no storage-not-empty signal received
                    // and we have wait enough time for element to store
                    break;
                }
            }
        } finally {
            lock.unlock();
        }

        return Collections.unmodifiableList(entries);
    }

    @Override
    public void store(List<byte[]> batch) throws StorageException {
        requireNonNull(batch, "batch");
        if (closed()) {
            throw new IllegalStateException("storage is closed");
        }

        if (batch.isEmpty()) {
            logger.warn("append with empty entries");
            return;
        }

        lock.lock();
        try {
            long id = getLastProducedId();

            for (byte[] bs : batch) {
                final SerializedObjectWithId<byte[]> e = new SerializedObjectWithId<>(++id, bs);
                if (makeRoomForEntry(false)) {
                    assert dataLogWriter != null;
                    dataLogWriter.append(encodeObjectWithId(e));
                    mm.add(e);
                } else {
                    throw new StorageException("no more room to storage data");
                }
            }
            assert dataLogWriter != null;
            dataLogWriter.flush(forceSyncOnFlushDataLogWriter);
            storageNotEmpty.signal();
        } catch (IOException ex) {
            throw new StorageException("append log on file based storage failed", ex);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws Exception {
        close(false);
    }

    void close(boolean force) throws Exception {
        CompletableFuture<Void> f = closeFuture;
        if (f == null) {
            lock.lockInterruptibly();
            try {
                f = closeFuture;
                if (f == null) {
                    f = new CompletableFuture<>();
                    closeFuture = f;
                    final PostClose task = new PostClose(f);
                    boolean shutdownNow = force;
                    if (!force) {
                        try {
                            sstableWriterPool.submit(task);
                        } catch (RejectedExecutionException unused) {
                            shutdownNow = true;
                        }
                    }

                    if (shutdownNow) {
                        sstableWriterPool.shutdownNow();
                        task.run();
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        assert f == closeFuture;
        f.join();
    }

    boolean closed() {
        return closeFuture != null;
    }

    /**
     * Use package access level only for testing.
     * Please note that usually it must be protected with lock.
     *
     * @return the last produced id
     */
    @VisibleForTesting
    long getLastProducedId() {
        if (!mm.isEmpty()) {
            return mm.lastId();
        } else if (imm != null && !imm.isEmpty()) {
            return imm.lastId();
        } else {
            return manifest.getLastId();
        }
    }

    /**
     * It's difficult to test this method throw some exception then stop the storage. So we set it's access level to package
     * to make test easier.
     *
     * @param immutableMemtable the immutable memtable to flush
     */
    @VisibleForTesting
    void writeMemtable(Memtable immutableMemtable) {
        logger.debug("start write mem table in background");
        boolean writeMemtableSuccess = false;
        try {
            ManifestRecord record = null;
            if (!immutableMemtable.isEmpty()) {
                assert immutableMemtable.firstId() > manifest.getLastId();
                final SSTableFileMetaInfo meta = writeMemTableToSSTable(immutableMemtable);
                record = ManifestRecord.newPlainRecord();
                record.addMeta(meta);
                record.setConsumerCommitLogFileNumber(consumerCommitLogFileNumber);
                record.setDataLogFileNumber(dataLogFileNumber);
                manifest.logRecord(record);
            }

            final Set<Integer> remainMetasFileNumberSet = manifest.searchMetas(Long.MIN_VALUE)
                    .stream()
                    .map(SSTableFileMetaInfo::getFileNumber)
                    .collect(Collectors.toSet());
            for (Integer fileNumber : tableCache.getAllFileNumbers()) {
                if (!remainMetasFileNumberSet.contains(fileNumber)) {
                    tableCache.evict(fileNumber);
                }
            }

            deleteOutdatedFiles(baseDir, dataLogFileNumber, consumerCommitLogFileNumber, tableCache);

            writeMemtableSuccess = true;
            logger.debug("write mem table in background done with manifest record {}", record);
        } catch (Throwable t) {
            logger.error("write memtable in background failed", t);
        } finally {
            lock.lock();
            try {
                if (!writeMemtableSuccess) {
                    safeClose();
                } else {
                    assert imm == immutableMemtable;
                    imm = null;
                }

                noPendingImmFlushing.signal();
            } finally {
                lock.unlock();
            }
        }
    }

    private class PostClose implements Runnable {
        private final CompletableFuture<Void> closeFuture;

        private PostClose(CompletableFuture<Void> closeFuture) {
            this.closeFuture = closeFuture;
        }

        @Override
        public void run() {
            lock.lock();
            try {
                try {
                    sstableWriterPool.shutdown();

                    if (dataLogWriter != null) {
                        dataLogWriter.close();
                    }

                    if (consumerCommitLogWriter != null) {
                        consumerCommitLogWriter.close();
                    }

                    manifest.close();

                    tableCache.evictAll();

                    releaseStorageLock();
                    logger.debug("File based storage shutdown successfully");
                    closeFuture.complete(null);
                } catch (Exception ex) {
                    closeFuture.completeExceptionally(ex);
                }
            } finally {
                lock.unlock();
            }
        }
    }

    private void createStorageDir() throws IOException {
        Path storageDirPath = Paths.get(baseDir);
        try {
            Files.createDirectories(storageDirPath);
        } catch (FileAlreadyExistsException ex) {
            // we don't care if the dir is already exists
        }
    }

    private FileLock lockStorage(String baseDir) throws IOException {
        final Path lockFilePath = Paths.get(baseDir, FileName.getLockFileName());
        final FileChannel lockChannel = FileChannel.open(lockFilePath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        FileLock fileLock = null;
        try {
            fileLock = lockChannel.tryLock();
            if (fileLock == null) {
                throw new IllegalStateException("failed to lock directory: " + baseDir);
            }
        } finally {
            if (fileLock == null || !fileLock.isValid()) {
                lockChannel.close();
            }
        }

        return fileLock;
    }

    private void releaseStorageLock() throws IOException {
        if (storageLock != null) {
            final Channel channel = storageLock.acquiredBy();
            try {
                storageLock.release();
            } finally {
                if (channel.isOpen()) {
                    channel.close();
                }
            }
        }
    }

    private void recoverStorage(Path currentFilePath, ManifestRecord record) throws IOException, StorageException {
        assert Files.exists(currentFilePath);
        final String currentManifestFileName = new String(Files.readAllBytes(currentFilePath), StandardCharsets.UTF_8);
        if (currentManifestFileName.isEmpty()) {
            throw new StorageException("empty CURRENT file in storage dir: " + baseDir);
        }

        manifest.recover(currentManifestFileName);

        recoverFromDataLogFiles(record);
        recoverLastConsumerCommittedId();
    }

    private void recoverFromDataLogFiles(ManifestRecord record) throws IOException, StorageException {
        final int dataLogFileNumber = manifest.getDataLogFileNumber();
        final List<FileName.FileNameMeta> dataLogFileMetas =
                FileName.getFileNameMetas(baseDir, fileMeta -> fileMeta.getType() == FileType.Log
                        && fileMeta.getFileNumber() >= dataLogFileNumber);

        for (int i = 0; i < dataLogFileMetas.size(); ++i) {
            final FileName.FileNameMeta fileMeta = dataLogFileMetas.get(i);
            recoverMemtableFromDataLogFiles(fileMeta.getFileNumber(), record, i == dataLogFileMetas.size() - 1);
        }
    }

    private void recoverMemtableFromDataLogFiles(int fileNumber, ManifestRecord record, boolean lastLogFile) throws IOException, StorageException {
        final Path logFilePath = Paths.get(baseDir, FileName.getLogFileName(fileNumber));
        if (!Files.exists(logFilePath)) {
            logger.warn("Log file {} was deleted. We can't recover memtable from it.", logFilePath);
            return;
        }

        final FileChannel readLogChannel = FileChannel.open(logFilePath, StandardOpenOption.READ);
        boolean flushedNewTable = false;
        Memtable recoveredMm = null;
        // 1. read all pending data from data log file
        try (LogReader reader = new LogReader(readLogChannel, true)) {
            while (true) {
                List<byte[]> logOpt = reader.readLog();
                if (!logOpt.isEmpty()) {
                    final SerializedObjectWithId<byte[]> e = decodeObjectWithId(logOpt);
                    if (recoveredMm == null) {
                        recoveredMm = new Memtable();
                    }
                    recoveredMm.add(e);
                    if (recoveredMm.getMemoryUsedInBytes() > Constant.kMaxMemtableSize) {
                        final SSTableFileMetaInfo meta = writeMemTableToSSTable(recoveredMm);
                        record.addMeta(meta);
                        flushedNewTable = true;
                        recoveredMm = null;
                    }
                } else {
                    break;
                }
            }

            if (lastLogFile && !flushedNewTable) {
                // 3. after read the last data log file, try to reuse this data log file
                // but we only reuse this old data log file when no sstable is flushed during reading this file
                assert dataLogWriter == null;
                assert dataLogFileNumber == 0;
                dataLogWriter = new LogWriter(logFilePath, readLogChannel.position(), StandardOpenOption.WRITE);
                dataLogFileNumber = fileNumber;
                if (recoveredMm != null) {
                    mm = recoveredMm;
                }
            } else if (recoveredMm != null) {
                // 2. flush pending data to sstable after read all the data from the log file
                final SSTableFileMetaInfo meta = writeMemTableToSSTable(recoveredMm);
                record.addMeta(meta);
            }
        } catch (BadRecordException ex) {
            logger.warn("got \"{}\" record in log file:\"{}\". ", ex.getType(), logFilePath);
        }
    }

    private void recoverLastConsumerCommittedId() throws IOException, StorageException {
        final int fileNumber = manifest.getConsumerCommittedIdLogFileNumber();
        final List<FileName.FileNameMeta> consumerLogFileMetas =
                FileName.getFileNameMetas(baseDir, fileMeta -> fileMeta.getType() == FileType.ConsumerCommit
                        && fileMeta.getFileNumber() >= fileNumber);

        for (int i = consumerLogFileMetas.size() - 1; i >= 0; --i) {
            final FileName.FileNameMeta fileMeta = consumerLogFileMetas.get(i);
            if (recoverLastConsumerCommittedIdFromLogFile(fileMeta.getFileNumber())) {
                break;
            }
        }
    }

    private boolean recoverLastConsumerCommittedIdFromLogFile(int fileNumber) throws IOException, StorageException {
        final Path logFilePath = Paths.get(baseDir, FileName.getConsumerCommittedIdFileName(fileNumber));
        if (!Files.exists(logFilePath)) {
            logger.warn("Log file {} was deleted. We can't recover consumer committed id from it.", logFilePath);
            return false;
        }

        final FileChannel ch = FileChannel.open(logFilePath, StandardOpenOption.READ);
        long readEndPosition;
        try (LogReader reader = new LogReader(ch, true)) {
            long id = lastCommittedId;
            while (true) {
                List<byte[]> logOpt = reader.readLog();
                if (!logOpt.isEmpty()) {
                    id = Bits.getLong(compact(logOpt), 0);
                } else {
                    break;
                }
            }

            readEndPosition = ch.position();

            if (id > lastCommittedId) {
                assert consumerCommitLogWriter == null;
                assert consumerCommitLogFileNumber == 0;
                consumerCommitLogWriter = new LogWriter(logFilePath, readEndPosition, StandardOpenOption.WRITE);
                consumerCommitLogFileNumber = fileNumber;
                lastCommittedId = id;
                return true;
            }
        } catch (BadRecordException ex) {
            logger.warn("got \"{}\" record in data log file:\"{}\". ", ex.getType(), logFilePath);
        }
        return false;
    }

    private byte[] encodeObjectWithId(SerializedObjectWithId<byte[]> obj) {
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + Integer.BYTES + obj.getSerializedObject().length);
        buffer.putLong(obj.getId());
        buffer.putInt(obj.getSerializedObject().length);
        buffer.put(obj.getSerializedObject());

        return buffer.array();
    }

    private SerializedObjectWithId<byte[]> decodeObjectWithId(List<byte[]> bytes) {
        final ByteBuffer buffer = ByteBuffer.wrap(compact(bytes));
        final long id = buffer.getLong();
        final int length = buffer.getInt();
        final byte[] bs = new byte[length];
        buffer.get(bs);

        return new SerializedObjectWithId<>(id, bs);
    }

    private byte[] compact(List<byte[]> output) {
        final int size = output.stream().mapToInt(b -> b.length).sum();
        final ByteBuffer buffer = ByteBuffer.allocate(size);
        for (byte[] bytes : output) {
            buffer.put(bytes);
        }
        return buffer.array();
    }

    private List<SerializedObjectWithId<byte[]>> doFetch(long fromId, int limit) throws StorageException {
        Itr itr;

        if (!mm.isEmpty() && fromId >= mm.firstId()) {
            return mm.getEntries(fromId, limit);
        }

        try {
            if (imm != null && fromId >= imm.firstId()) {
                final List<SeekableIterator<Long, SerializedObjectWithId<byte[]>>> itrs = Arrays.asList(imm.iterator(), mm.iterator());
                for (SeekableIterator<Long, SerializedObjectWithId<byte[]>> it : itrs) {
                    it.seek(fromId);
                }

                itr = new Itr(itrs);
            } else {
                itr = internalIterator(fromId);
            }

            final List<SerializedObjectWithId<byte[]>> ret = new ArrayList<>();
            while (itr.hasNext()) {
                SerializedObjectWithId<byte[]> e = itr.next();
                if (ret.size() >= limit) {
                    break;
                }

                ret.add(e);
            }

            return ret;
        } catch (StorageRuntimeException ex) {
            throw new StorageException(ex.getMessage(), ex.getCause());
        }
    }

    private boolean makeRoomForEntry(boolean force) throws IOException, StorageException {
        try {
            boolean forceRun = force;
            while (true) {
                if (closed()) {
                    return false;
                }

                if (forceRun || mm.getMemoryUsedInBytes() > Constant.kMaxMemtableSize) {
                    if (imm != null) {
                        noPendingImmFlushing.await();
                        continue;
                    }

                    forceRun = false;
                    makeRoomForEntry0();
                } else {
                    return true;
                }
            }
        } catch (InterruptedException t) {
            // Restore interrupted state and throw StorageException
            Thread.currentThread().interrupt();
            throw new StorageException("thread was interrupted when waiting room for new entry");
        }
    }

    private void makeRoomForEntry0() throws IOException, StorageException {
        final LogWriter logWriter = createNewDataLogWriter();
        if (dataLogWriter != null) {
            dataLogWriter.close();
        }
        dataLogWriter = logWriter;

        final LogWriter consumerWriter = createConsumerCommitLogWriter();
        if (consumerCommitLogWriter != null) {
            consumerCommitLogWriter.close();
        }
        consumerCommitLogWriter = consumerWriter;

        imm = mm;
        mm = new Memtable();
        logger.debug("Trigger compaction, new log file number={}", dataLogFileNumber);
        try {
            sstableWriterPool.submit(() -> writeMemtable(imm));
        } catch (RejectedExecutionException ex) {
            throw new StorageException("flush memtable task was rejected", ex);
        }
    }

    private LogWriter createNewDataLogWriter() throws IOException {
        final int nextLogFileNumber = manifest.getNextFileNumber();
        final String nextLogFile = FileName.getLogFileName(nextLogFileNumber);
        final LogWriter writer = new LogWriter(Paths.get(baseDir, nextLogFile),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        dataLogFileNumber = nextLogFileNumber;
        return writer;
    }

    private LogWriter createConsumerCommitLogWriter() throws IOException {
        final int nextLogFileNumber = manifest.getNextFileNumber();
        final String nextLogFile = FileName.getConsumerCommittedIdFileName(nextLogFileNumber);
        final LogWriter writer = new LogWriter(Paths.get(baseDir, nextLogFile),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        consumerCommitLogFileNumber = nextLogFileNumber;
        return writer;
    }

    private SSTableFileMetaInfo writeMemTableToSSTable(Memtable mm) throws IOException {
        final SSTableFileMetaInfo meta = new SSTableFileMetaInfo();

        final int fileNumber = manifest.getNextFileNumber();
        meta.setFileNumber(fileNumber);
        meta.setFirstId(mm.firstId());
        meta.setLastId(mm.lastId());

        final String tableFileName = FileName.getSSTableName(fileNumber);
        final Path tableFile = Paths.get(baseDir, tableFileName);
        try (FileChannel ch = FileChannel.open(tableFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            final TableBuilder tableBuilder = new TableBuilder(ch);

            for (SerializedObjectWithId<byte[]> entry : mm) {
                final byte[] data = entry.getSerializedObject();
                tableBuilder.add(entry.getId(), data);
            }

            long tableFileSize = tableBuilder.finishBuild();

            if (tableFileSize > 0) {
                meta.setFileSize(tableFileSize);

                ch.force(true);
            }
        }

        tableCache.loadTable(fileNumber, meta.getFileSize());

        if (meta.getFileSize() <= 0) {
            Files.deleteIfExists(tableFile);
        }

        return meta;
    }

    private Itr internalIterator(long start) throws StorageException {
        List<SeekableIterator<Long, SerializedObjectWithId<byte[]>>> itrs = getSSTableIterators(start);
        if (imm != null) {
            itrs.add(imm.iterator().seek(start));
        }
        itrs.add(mm.iterator().seek(start));
        for (SeekableIterator<Long, SerializedObjectWithId<byte[]>> itr : itrs) {
            itr.seek(start);
            if (itr.hasNext()) {
                break;
            }
        }

        return new Itr(itrs);
    }

    private List<SeekableIterator<Long, SerializedObjectWithId<byte[]>>> getSSTableIterators(long start) throws StorageException {
        try {
            List<SSTableFileMetaInfo> metas = manifest.searchMetas(start);
            List<SeekableIterator<Long, SerializedObjectWithId<byte[]>>> ret = new ArrayList<>(metas.size());
            for (SSTableFileMetaInfo meta : metas) {
                ret.add(tableCache.iterator(meta.getFileNumber(), meta.getFileSize()));
            }
            return ret;
        } catch (IOException ex) {
            throw new StorageException(
                    String.format("get sstable iterators start: %s from SSTable failed", start), ex);
        }
    }

    private void tryTruncate() {
        try {
            final long lastCommittedId = getLastCommittedId();
            final long truncateId = Math.max(0, lastCommittedId);
            manifest.truncateToId(truncateId);
            lastTryTruncateTime = System.nanoTime();
        } catch (Exception ex) {
            logger.error("Truncate handler failed for entry", ex);
        }
    }

    private void safeClose() {
        lock.lock();
        try {
            if (safeCloseThread == null) {
                final Thread t = safeCloseThreadFactory.newThread(() -> {
                    try {
                        close();
                    } catch (Exception ex) {
                        logger.error("Close storage under directory: {} failed", baseDir, ex);
                    }
                });

                t.setDaemon(true);
                t.start();

                safeCloseThread = t;
            }
        } finally {
            lock.unlock();
        }
    }

    private void blockSafeClose() {
        safeClose();
        try {
            assert safeCloseThread != null;
            safeCloseThread.join();
        } catch (InterruptedException ex) {
            // ignore InterruptedException and restore interrupted state...
            Thread.currentThread().interrupt();
        }
    }

    private static void deleteOutdatedFiles(String baseDir,
                                            int dataLogFileNumber,
                                            int consumerCommittedIdLogFileNumber,
                                            TableCache tableCache) {
        final List<Path> outdatedFilePaths = scanFiles(baseDir, meta -> {
            switch (meta.getType()) {
                case ConsumerCommit:
                    return meta.getFileNumber() < consumerCommittedIdLogFileNumber;
                case Log:
                    return meta.getFileNumber() < dataLogFileNumber;
                case SSTable:
                    return !tableCache.hasTable(meta.getFileNumber());
                case Current:
                case Lock:
                case TempManifest:
                case Manifest:
                case Unknown:
                default:
                    return false;
            }
        });

        deleteFiles(outdatedFilePaths);
    }

    private static void cleanWorkingDirectory(String baseDir) {
        final List<Path> filesToDelete = scanFiles(baseDir, meta -> {
            switch (meta.getType()) {
                case ConsumerCommit:
                case Log:
                case SSTable:
                case Current:
                case Lock:
                case TempManifest:
                case Manifest:
                    return true;
                case Unknown:
                default:
                    return false;
            }
        });

        deleteFiles(filesToDelete);
    }

    private static List<Path> scanFiles(String baseDir, Predicate<? super FileNameMeta> filter) {
        final File dirFile = new File(baseDir);
        final File[] files = dirFile.listFiles();

        if (files != null) {
            return Arrays.stream(files)
                    .filter(File::isFile)
                    .map(File::getName)
                    .map(FileName::parseFileName)
                    .filter(filter)
                    .map(meta -> Paths.get(baseDir, meta.getFileName()))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private static void deleteFiles(List<Path> filesToDelete) {
        try {
            for (Path path : filesToDelete) {
                Files.deleteIfExists(path);
            }
        } catch (IOException t) {
            logger.error("delete files:{} failed", filesToDelete, t);
        }
    }
}
