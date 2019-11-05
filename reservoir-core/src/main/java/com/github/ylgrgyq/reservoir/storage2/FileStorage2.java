package com.github.ylgrgyq.reservoir.storage2;

import com.github.ylgrgyq.reservoir.ObjectQueueStorage;
import com.github.ylgrgyq.reservoir.SerializedObjectWithId;
import com.github.ylgrgyq.reservoir.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
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

import static com.github.ylgrgyq.reservoir.FileUtils.lockDirectory;
import static com.github.ylgrgyq.reservoir.FileUtils.releaseDirectoryLock;
import static java.util.Objects.requireNonNull;

public class FileStorage2 implements ObjectQueueStorage<byte[]> {
    private static final Logger logger = LoggerFactory.getLogger(FileStorage2.class.getName());
    // Todo: remove this constant segment size
    private static final int DEFAULT_SEGMENT_SIZE = 1024;

    private Path baseDirPath;
    private long lastCommittedId;
    private FileLock storageLock;
    private final Lock lock;
    private final Condition storageNotEmpty;

    private Log dataLog;
    private Log commitIdLog;

    public FileStorage2(String storageBaseDir) throws StorageException {
        requireNonNull(storageBaseDir, "storageBaseDir");
        Path baseDirPath = Paths.get(storageBaseDir);

        if (Files.exists(baseDirPath) && !Files.isDirectory(baseDirPath)) {
            throw new IllegalArgumentException("\"" + storageBaseDir + "\" must be a directory");
        }

        this.baseDirPath = baseDirPath;
        this.lock = new ReentrantLock();
        this.storageNotEmpty = lock.newCondition();
        this.lastCommittedId = -1;
        boolean initStorageSuccess = false;
        try {
            Files.createDirectories(baseDirPath);

            this.storageLock = lockDirectory(baseDirPath, FileName.getLockFileName());

            logger.debug("Start init storage under {}", storageBaseDir);

            this.dataLog = new Log(baseDirPath, "DataLog", DEFAULT_SEGMENT_SIZE);
            this.commitIdLog = new Log(baseDirPath, "CommitIdLog", DEFAULT_SEGMENT_SIZE);
            initStorageSuccess = true;
        } catch (IOException | OverlappingFileLockException ex) {
            throw new StorageException("init storage under: " + storageBaseDir + " failed", ex);
        } finally {
            if (!initStorageSuccess) {
                try {
                    releaseDirectoryLock(storageLock);
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
                ByteUtils.writeVarlong(id, ByteBuffer.allocate(ByteUtils.sizeOfVarlong(id)));
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

    }

    boolean closed() {
        return false;
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
                    final byte[] val = new byte[record.valueSize()];
                    record.value().get(val);
                    entries.add(new SerializedObjectWithId<>(record.id(), val));
                    if (entries.size() == limit) {
                        break;
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
