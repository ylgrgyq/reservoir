package com.github.ylgrgyq.reservoir.storage2;

import com.github.ylgrgyq.reservoir.ObjectQueueStorage;
import com.github.ylgrgyq.reservoir.SerializedObjectWithId;
import com.github.ylgrgyq.reservoir.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public final class FileStorage2 implements ObjectQueueStorage<byte[]> {
    private static final Logger logger = LoggerFactory.getLogger(FileStorage2.class.getName());
    private static final String dataLogName = "DataLog";
    private static final String commitIdLogName = "CommitIdLog";

    // Todo: remove this constant segment size
    private static final int DEFAULT_SEGMENT_SIZE = 1024;

    private long lastCommittedId;
    private StorageLock storageLock;
    private final Lock lock;
    private final Condition storageNotEmpty;

    private boolean closed;
    private Log dataLog;
    private Log commitIdLog;

    public FileStorage2(String storageBaseDir) throws StorageException {
        requireNonNull(storageBaseDir, "storageBaseDir");
        Path baseDirPath = Paths.get(storageBaseDir);

        if (Files.exists(baseDirPath) && !Files.isDirectory(baseDirPath)) {
            throw new IllegalArgumentException("\"" + storageBaseDir + "\" must be a directory");
        }

        this.lock = new ReentrantLock();
        this.storageNotEmpty = lock.newCondition();
        this.lastCommittedId = -1;
        this.storageLock = new StorageLock(baseDirPath);
        boolean initStorageSuccess = false;
        try {
            Files.createDirectories(baseDirPath);

            this.storageLock.lock();

            logger.debug("Start init storage under {}", storageBaseDir);

            this.dataLog = new Log(baseDirPath, dataLogName, DEFAULT_SEGMENT_SIZE);
            this.commitIdLog = new Log(baseDirPath, commitIdLogName, DEFAULT_SEGMENT_SIZE);
            this.lastCommittedId = lastCommittedIdFromCommitIdLog();
            initStorageSuccess = true;
        } catch (IOException | OverlappingFileLockException ex) {
            throw new StorageException("init storage under: " + storageBaseDir + " failed", ex);
        } finally {
            if (!initStorageSuccess) {
                try {
                    this.storageLock.unlock();
                } catch (IOException ex) {
                    throw new StorageException(ex);
                }
            }
        }
    }

    @Override
    public void store(List<byte[]> batch) throws StorageException {
        requireNonNull(batch, "batch");

        if (batch.isEmpty()) {
            logger.warn("append with empty entries");
            return;
        }

        lock.lock();
        ensureStorageOpen();
        try {
            dataLog.append(batch.stream().map(ByteBuffer::wrap).collect(Collectors.toList()));
            storageNotEmpty.signal();
        } catch (IOException ex) {
            throw new StorageException("append log on file based storage failed", ex);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commitId(long id) throws StorageException {
        lock.lock();
        ensureStorageOpen();

        try {
            if (id > lastCommittedId) {
                final ByteBuffer idBuffer = ByteBuffer.allocate(ByteUtils.sizeOfVarlong(id));
                ByteUtils.writeVarlong(id, idBuffer);
                idBuffer.flip();
                commitIdLog.append(Collections.singletonList(idBuffer));
                lastCommittedId = id;
            }
        } catch (IOException ex) {
            throw new StorageException(ex);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long getLastCommittedId() {
        return lastCommittedId;
    }

    @Override
    public List<SerializedObjectWithId<byte[]>> fetch(long fromId, int limit) throws InterruptedException, StorageException {
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
    public void close() throws Exception {
        lock.lockInterruptibly();
        try {
            if (closed()) {
                return;
            }
            closed = true;
            storageNotEmpty.signal();
            dataLog.close();
            commitIdLog.close();
            storageLock.unlock();
        } finally {
            lock.unlock();
        }
    }

    private long lastCommittedIdFromCommitIdLog() throws IOException {
        FileRecords records = this.commitIdLog.read(this.commitIdLog.lastId());
        if (records == null) {
            return -1L;
        }

        Record lastRecord = null;
        for (Record record : records.records()) {
            lastRecord = record;
        }

        if (lastRecord == null) {
            return -1L;
        }

        return ByteUtils.readVarlong(lastRecord.value());
    }

    private boolean closed() {
        return closed;
    }

    private void ensureStorageOpen() {
        if (closed()) {
            throw new IllegalStateException("storage is closed");
        }
    }

    private List<SerializedObjectWithId<byte[]>> doFetch(long fromId, int limit) throws StorageException {
        assert limit > 0 : "limit: " + limit;
        ensureStorageOpen();
        try {
            final FileRecords records = dataLog.read(fromId + 1);
            if (records != null) {
                final List<SerializedObjectWithId<byte[]>> entries = new ArrayList<>(limit);
                for (Record record : records.records()) {
                    if (record.id() > fromId) {
                        final byte[] val = new byte[record.valueSize()];
                        record.value().get(val);
                        entries.add(new SerializedObjectWithId<>(record.id(), val));
                        if (entries.size() == limit) {
                            break;
                        }
                    }
                }
                return entries;
            }
        } catch (IOException ex) {
            throw new StorageException(ex);
        }

        return Collections.emptyList();
    }
}
