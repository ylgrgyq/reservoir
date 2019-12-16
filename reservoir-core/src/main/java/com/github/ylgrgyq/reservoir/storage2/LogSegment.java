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

public final class LogSegment implements Closeable {
    /**
     * Recover a {@code LogSegment} from the target path.
     *
     * @param segmentPath the path to recover a {@code LogSegment}
     * @return the recovered {@code LogSegment}
     * @throws StorageRuntimeException when the segment file to recover is corrupted
     * @throws EmptySegmentException   when the recovered segment is empty
     */
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
                assert batch.baseId() >= 0;
                if (startId == -1) {
                    startId = batch.baseId();
                }

                lastId = batch.lastId();

                if (startId > lastId) {
                    throw new InvalidSegmentException("a record batch in segment has last id: " + lastId +
                            " smaller than the start id (" + startId + ") of the segment");
                }
            }
        } catch (Exception ex) {
            throw new StorageRuntimeException("recover from segment file: " + segmentPath + " failed", ex);
        }

        if (startId < 0) {
            throw new EmptySegmentException("found empty segment under path: " + segmentPath);
        }

        return new LogSegment(segmentPath, startId, lastId, fileRecords, createT);
    }

    /**
     * Create a new segment at a target path with an specific start id
     *
     * @param segmentFilePath the path of a file to create the new segment
     * @param startId         the start id of the new segment
     * @return the new segment
     * @throws IOException      if any I/O error occur
     * @throws StorageException if {@code segmentFilePath} points to an already exits file
     */
    static LogSegment newSegment(Path segmentFilePath, long startId) throws IOException {
        if (Files.exists(segmentFilePath)) {
            throw new StorageException("segment on path " + segmentFilePath + " is already exists");
        }

        final FileRecords fileRecords = FileRecords.open(segmentFilePath.toFile());
        final BasicFileAttributes attr = Files.readAttributes(segmentFilePath, BasicFileAttributes.class);
        final Instant createT = attr.creationTime().toInstant();

        return new LogSegment(segmentFilePath, startId, startId - 1, fileRecords, createT);
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

    /**
     * Append the input group of {@link ByteBuffer}s into this segment.
     *
     * @param batch the input group of {@link ByteBuffer}s
     * @throws IOException if any I/O error occur
     */
    public void append(List<ByteBuffer> batch) throws IOException {
        final MemoryRecords records = new MemoryRecords(lastId + 1, batch);
        fileRecords.append(records);
        lastId = records.lastId();
    }

    /**
     * Get the start id of this segment, inclusive;
     *
     * @return the start id of this segment
     */
    long startId() {
        return startId;
    }

    /**
     * Get the last id of this segment, inclusive;
     *
     * @return the last id of this segment
     */
    public long lastId() {
        return lastId;
    }

    /**
     * Get the total size in bytes of this segment
     *
     * @return the total size in bytes of this segment
     */
    public int size() {
        return fileRecords.totalSizeInBytes();
    }

    /**
     * Get the creation time of this segment. It equals to the creation time of the underlying file of this segment.
     *
     * @return the creation time of this segment.
     */
    public Instant creationTime() {
        return creationTime;
    }

    /**
     * Get a {@link FileRecords} slice contains a record with id equals to {@code fromId} from this segment.
     * If the {@code fromId} is not in this segment, {@code null} will be returned.
     * <p>
     * Note that if {@code fromId} exits in this segment, this method only insure that the returned
     * {@link FileRecords} contains the record with {@code fromId} but it may not be the first record
     * in this {@link FileRecords}.
     *
     * @param fromId the inclusive start id of the returned {@link FileRecords}
     * @return a {@link FileRecords} slice contains a record with the id equals to {@code fromId}
     * @throws IOException if any I/O error occur
     */
    @Nullable
    public FileRecords records(long fromId) throws IOException {
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

    Path segmentFilePath() {
        return segmentPath;
    }

    FileRecords fileRecords() {
        return fileRecords;
    }
}
