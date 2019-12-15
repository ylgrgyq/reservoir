package com.github.ylgrgyq.reservoir.storage2;

import com.github.ylgrgyq.reservoir.storage2.FileLogInputStream.RecordBatchWithPosition;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

public class FileRecords extends AbstractRecords implements Closeable {
    public static FileRecords open(File file) throws IOException {
        final FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
        return new FileRecords(file, channel, 0, Integer.MAX_VALUE, false);
    }

    private int size;
    // Todo: this field seems no use
    private final File file;
    private final FileChannel channel;
    private final int start;
    private final int end;
    // Todo: Maybe we can use a subclass of FileRecords to remove this flag
    private final boolean isSlice;

    private FileRecords(File file, FileChannel channel, int start, int end, boolean isSlice) throws IOException {
        this.file = file;
        this.channel = channel;
        this.start = start;
        this.end = end;
        this.isSlice = isSlice;
        if (isSlice) {
            this.size = end - start;
        } else {
            if (channel.size() > Integer.MAX_VALUE) {
                // do not expect recovery so we throw unchecked exception
                throw new StorageRuntimeException("The size of file records " + file + " (" + channel.size() +
                        ") is larger than the maximum allowed size of " + Integer.MAX_VALUE);
            }

            final int limit = (int) (channel.size());
            channel.position(limit);

            this.size = limit;
        }
    }

    FileRecords slice(int position, int size) throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException("position: " + position + " (expected: >= 0)");
        }

        if (position > totalSizeInBytes() - start) {
            throw new IllegalArgumentException("position: " + position +
                    " (expected between: [" + start + ", " + (totalSizeInBytes() - start) + ") )");
        }

        if (size < 0) {
            throw new IllegalArgumentException("size: " + size + " (expected: >= 0)");
        }

        int end = position + start + size;
        // handle integer overflow or end is beyond the the size of this file records
        if (end < 0 || end > start + totalSizeInBytes()) {
            end = start + totalSizeInBytes();
        }

        return new FileRecords(file, channel, start + position, end, true);
    }

    public long append(MemoryRecords records) throws IOException {
        if (isSlice) {
            throw new IllegalStateException("append is not allowed on a slice");
        }

        if (records.totalSizeInBytes() > Integer.MAX_VALUE - size) {
            throw new IllegalArgumentException("Records size: " + records.totalSizeInBytes()
                    + " is too large for file segment with position at: " + size);
        }

        size += records.writeFullyTo(channel);
        return records.lastId();
    }

    @Override
    public int totalSizeInBytes() {
        return size;
    }

    FileChannel channel() {
        return channel;
    }

    @Override
    public Iterable<RecordBatchWithPosition> batches() {
        return () -> batchIterator(start);
    }

    public Iterable<RecordBatchWithPosition> batches(int startSearchingPos) {
        return () -> batchIterator(startSearchingPos);
    }

    @Nullable
    Integer searchRecordBatchStartPosForId(int startSearchingPos, long targetId) {
        for (RecordBatchWithPosition batch : batches(startSearchingPos)) {
            if (batch.lastId() >= targetId) {
                return batch.position();
            }
        }

        return null;
    }

    public void flush() throws IOException {
        channel.force(true);
    }

    @Override
    public void close() throws IOException {
        flush();
        if (!isSlice) {
            channel.close();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final FileRecords that = (FileRecords) o;
        // we do not compare channel field because if file, start, end, isSlice is equal,
        // we can sure that the two FileRecords will produce the same Records so they are logically equivalent
        return size == that.size &&
                start == that.start &&
                end == that.end &&
                isSlice == that.isSlice &&
                file.equals(that.file);
    }

    @Override
    public int hashCode() {
        return Objects.hash(size, file, channel, start, end, isSlice);
    }

    @Override
    public String toString() {
        return "FileRecords{" +
                "file=" + file +
                ", size=" + size +
                ", start=" + start +
                ", end=" + end +
                ", isSlice=" + isSlice +
                '}';
    }

    private AbstractIterator<RecordBatchWithPosition> batchIterator(int startSearchingPos) {
        final int start = Math.max(startSearchingPos, this.start);
        int end = totalSizeInBytes();
        if (isSlice) {
            end = this.end;
        }
        final FileLogInputStream inputStream = new FileLogInputStream(this, start, end);
        return new RecordBatchIterator<>(inputStream);
    }
}
