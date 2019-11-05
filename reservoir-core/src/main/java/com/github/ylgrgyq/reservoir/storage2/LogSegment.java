package com.github.ylgrgyq.reservoir.storage2;

import com.github.ylgrgyq.reservoir.StorageException;
import com.github.ylgrgyq.reservoir.storage2.FileLogInputStream.RecordBatchWithPosition;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

public class LogSegment implements Closeable {
    static LogSegment fromFilePath(Path segmentPath) {
        assert segmentPath.toFile().exists();
        final Instant createT;
        final FileRecords fileRecords;
        long startId = -1;
        long lastId = -1;
        try {
            final BasicFileAttributes attr = Files.readAttributes(segmentPath, BasicFileAttributes.class);
            createT = attr.creationTime().toInstant();
            fileRecords = FileRecords.open(segmentPath.toFile());

            for (RecordBatchWithPosition batch : fileRecords.batches()) {
                assert batch.baseId() > 0;
                if (startId < 0) {
                    startId = batch.baseId();
                }

                lastId = batch.lastId();
            }
        } catch (Exception ex) {
            throw new StorageRuntimeException("recover from segment file: " + segmentPath + " failed", ex);
        }

        if (startId < 0) {
            throw new EmptySegmentException("found empty segment under path: " + segmentPath);
        } else {
            return new LogSegment(segmentPath, startId, lastId, fileRecords, createT);
        }

    }

    static LogSegment newSegment(Path segmentFilePath, long startId) throws IOException {
        if (Files.exists(segmentFilePath)) {
            throw new StorageException("segment on path " + segmentFilePath + " is alread exists");
        }

        return new LogSegment(segmentFilePath, startId, startId, FileRecords.open(segmentFilePath.toFile()), Instant.now());
    }

    private final Path segmentPath;
    private final FileRecords fileRecords;
    private final long startId;
    private final Instant creationTime;
    private long lastId;

    private LogSegment(Path segmentPath, long startId, long lastId, FileRecords fileRecords, Instant creationTime) {
        this.segmentPath = segmentPath;
        this.startId = startId;
        this.lastId = lastId;
        this.fileRecords = fileRecords;
        this.creationTime = creationTime;
    }

    public void append(List<ByteBuffer> batch) throws IOException {
        final MemoryRecords records = new MemoryRecords(lastId, batch);
        fileRecords.append(records);
        lastId = records.lastId();
    }

    long startId() {
        return startId;
    }

    public long lastId() {
        return lastId;
    }

    public int size() {
        return fileRecords.totalSizeInBytes();
    }

    public Instant creationTime() {
        return creationTime;
    }

    /**
     * @param fromId inclusive
     * @return
     * @throws IOException
     */
    @Nullable
    public FileRecords records(long fromId) throws IOException {
        assert fromId <= lastId : "lastId: " + lastId + " fromId: " + fromId;

        final Integer pos = fileRecords.searchRecordBatchStartPosForId(0, fromId);
        if (pos != null) {
            return fileRecords.slice(pos, fileRecords.totalSizeInBytes());
        } else {
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        fileRecords.close();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final LogSegment segment = (LogSegment) o;
        return startId == segment.startId &&
                lastId == segment.lastId &&
                fileRecords.equals(segment.fileRecords) &&
                creationTime.equals(segment.creationTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileRecords, startId, creationTime, lastId);
    }

    @Override
    public String toString() {
        return "LogSegment{" +
                "path=" + segmentPath +
                ", startId=" + startId +
                ", lastId=" + lastId +
                ", creationTime=" + creationTime +
                '}';
    }

    FileRecords fileRecords() {
        return fileRecords;
    }
}
