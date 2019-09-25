package com.github.ylgrgyq.reservoir.storage;

import com.github.ylgrgyq.reservoir.StorageException;
import com.github.ylgrgyq.reservoir.storage.ManifestRecord.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

final class Manifest {
    private static final Logger logger = LoggerFactory.getLogger(Manifest.class.getName());

    private final String baseDir;
    private final List<SSTableFileMetaInfo> metas;
    private final ReadLock readMetasLock;
    private final WriteLock writeMetasLock;

    @Nullable
    private LogWriter manifestRecordWriter;
    private int nextFileNumber = 1;
    private int dataLogFileNumber;
    private int consumerCommittedIdLogFileNumber;

    Manifest(String baseDir) {
        this.baseDir = baseDir;
        this.metas = new ArrayList<>();
        ReentrantReadWriteLock metasLock = new ReentrantReadWriteLock();
        this.readMetasLock = metasLock.readLock();
        this.writeMetasLock = metasLock.writeLock();
    }

    synchronized void logRecord(ManifestRecord record) throws IOException {
        assert record.getType() != ManifestRecord.Type.PLAIN || record.getDataLogFileNumber() >= dataLogFileNumber;

        registerMetas(record);

        String manifestFileName = null;
        int manifestFileNumber = 0;
        if (manifestRecordWriter == null) {
            final int fileNumber = getNextFileNumber();
            manifestFileNumber = fileNumber;
            manifestFileName = FileName.getManifestFileName(fileNumber);
            manifestRecordWriter = new LogWriter(Paths.get(baseDir, manifestFileName),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        }

        if (record.getType() == Type.PLAIN) {
            record.setNextFileNumber(nextFileNumber);
        }
        manifestRecordWriter.append(record.encode());
        manifestRecordWriter.flush(true);

        logger.debug("written manifest record {} to manifest file number {}", record, manifestFileNumber);

        // only set CURRENT to the new manifest file after a new record has safely written to it
        if (manifestFileName != null) {
            assert manifestFileNumber != 0;
            FileName.setCurrentFile(baseDir, manifestFileNumber);
        }

        if (record.getType() == Type.PLAIN) {
            dataLogFileNumber = record.getDataLogFileNumber();
            consumerCommittedIdLogFileNumber = record.getConsumerCommitLogFileNumber();
        }
    }

    synchronized void recover(String manifestFileName) throws IOException, StorageException {
        final Path manifestFilePath = Paths.get(baseDir, manifestFileName);
        if (!Files.exists(manifestFilePath)) {
            throw new StorageException("CURRENT file points to an non-exists manifest file: " +
                    baseDir + File.separator + manifestFileName);
        }

        final FileChannel manifestFile = FileChannel.open(manifestFilePath, StandardOpenOption.READ);
        final List<SSTableFileMetaInfo> ms = new ArrayList<>();
        try (LogReader reader = new LogReader(manifestFile, true)) {
            while (true) {
                final CompositeBytesReader bytesReader = reader.readLog();
                if (!bytesReader.isEmpty()) {
                    final ManifestRecord record = ManifestRecord.decode(bytesReader);
                    switch (record.getType()) {
                        case PLAIN:
                            nextFileNumber = record.getNextFileNumber();
                            dataLogFileNumber = record.getDataLogFileNumber();
                            consumerCommittedIdLogFileNumber = record.getConsumerCommitLogFileNumber();
                            break;
                        case REPLACE_METAS:
                            ms.clear();
                            break;
                        default:
                            throw new StorageException("unknown manifest record type:" + record.getType());
                    }
                    ms.addAll(record.getMetas());
                } else {
                    break;
                }
            }

            writeMetasLock.lock();
            try {
                // we must make sure that searchMetas will only be called after recovery
                assert metas.isEmpty();
                metas.addAll(ms);
            } finally {
                writeMetasLock.unlock();
            }
        } catch (BadRecordException ex) {
            String msg = String.format("recover manifest from file:\"%s\" failed due to \"%s\" log record",
                    manifestFileName, manifestFilePath);
            throw new StorageException(msg);
        }
    }

    long getFirstId() {
        readMetasLock.lock();
        try {
            if (!metas.isEmpty()) {
                return metas.get(0).getFirstId();
            } else {
                return -1L;
            }
        } finally {
            readMetasLock.unlock();
        }
    }

    long getLastId() {
        readMetasLock.lock();
        try {
            if (!metas.isEmpty()) {
                return metas.get(metas.size() - 1).getLastId();
            } else {
                return -1L;
            }
        } finally {
            readMetasLock.unlock();
        }
    }

    void truncateToId(long toId) throws IOException {
        writeMetasLock.lock();
        try {
            if (toId > 0) {
                List<SSTableFileMetaInfo> remainMetas = searchMetas(toId);
                if (remainMetas.size() < metas.size()) {
                    ManifestRecord record = ManifestRecord.newReplaceAllExistedMetasRecord();
                    record.addMetas(remainMetas);
                    logRecord(record);

                    registerMetas(record);
                } else {
                    assert remainMetas.size() == metas.size();
                }
            }
        } finally {
            writeMetasLock.unlock();
        }
    }

    synchronized void close() throws IOException {
        if (manifestRecordWriter != null) {
            manifestRecordWriter.close();
        }
    }

    synchronized int getNextFileNumber() {
        return nextFileNumber++;
    }

    synchronized int getDataLogFileNumber() {
        return dataLogFileNumber;
    }

    synchronized int getConsumerCommittedIdLogFileNumber() {
        return consumerCommittedIdLogFileNumber;
    }

    /**
     * Find all the SSTableFileMetaInfo who's id range covers startId.
     *
     * @param startId target start id (inclusive)
     * @return iterator for found SSTableFileMetaInfo
     */
    List<SSTableFileMetaInfo> searchMetas(long startId) {
        readMetasLock.lock();
        try {
            int startMetaIndex;
            if (metas.size() > 32) {
                startMetaIndex = binarySearchStartMeta(startId);
            } else {
                startMetaIndex = traverseSearchStartMeta(startId);
            }

            return metas.subList(startMetaIndex, metas.size());
        } finally {
            readMetasLock.unlock();
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Manifest manifest = (Manifest) o;
        return getNextFileNumber() == manifest.getNextFileNumber() &&
                getDataLogFileNumber() == manifest.getDataLogFileNumber() &&
                getConsumerCommittedIdLogFileNumber() == manifest.getConsumerCommittedIdLogFileNumber() &&
                baseDir.equals(manifest.baseDir) &&
                metas.equals(manifest.metas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseDir, metas, getNextFileNumber(),
                getDataLogFileNumber(), getConsumerCommittedIdLogFileNumber());
    }

    private int traverseSearchStartMeta(long index) {
        int i = 0;
        while (i < metas.size()) {
            SSTableFileMetaInfo meta = metas.get(i);
            if (index <= meta.getFirstId()) {
                break;
            } else if (index <= meta.getLastId()) {
                break;
            }
            ++i;
        }

        return i;
    }

    private int binarySearchStartMeta(long index) {
        int start = 0;
        int end = metas.size();

        while (start < end) {
            int mid = (start + end) / 2;
            SSTableFileMetaInfo meta = metas.get(mid);
            if (index >= meta.getFirstId() && index <= meta.getLastId()) {
                return mid;
            } else if (index < meta.getFirstId()) {
                end = mid;
            } else {
                start = mid + 1;
            }
        }

        return start;
    }

    private void registerMetas(ManifestRecord record) {
        writeMetasLock.lock();
        try {
            if (record.getType() == ManifestRecord.Type.REPLACE_METAS) {
                this.metas.clear();
            }
            this.metas.addAll(record.getMetas());
        } finally {
            writeMetasLock.unlock();
        }
    }
}
