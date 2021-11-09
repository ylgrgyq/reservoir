package com.github.ylgrgyq.reservoir.storage2;

import com.github.ylgrgyq.reservoir.storage2.FileLogInputStream.RecordBatchWithPosition;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Objects;

import static com.github.ylgrgyq.reservoir.storage2.RecordsHeaderConstants.*;

/**
 * A log input stream which is backed by a {@link FileChannel}.
 */
public final class FileLogInputStream implements LogInputStream<RecordBatchWithPosition> {
    private static void readFully(FileChannel channel, ByteBuffer destinationBuffer, long position) throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException("The file channel position cannot be negative, but it is " + position);
        }
        long currentPosition = position;
        int bytesRead;
        do {
            bytesRead = channel.read(destinationBuffer, currentPosition);
            currentPosition += bytesRead;
        } while (bytesRead != -1 && destinationBuffer.hasRemaining());
    }

    private static void readFullyOrFail(FileChannel channel, ByteBuffer destinationBuffer, long position,
                                        String description) throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException("The file channel position cannot be negative, but it is " + position);
        }
        final int expectedReadBytes = destinationBuffer.remaining();
        readFully(channel, destinationBuffer, position);
        if (destinationBuffer.hasRemaining()) {
            throw new EOFException(String.format("Failed to read `%s` from file channel `%s`. Expected to read %d bytes, " +
                            "but reached end of file after reading %d bytes. Started read from position %d.",
                    description, channel, expectedReadBytes, expectedReadBytes - destinationBuffer.remaining(), position));
        }
    }

    private final int end;
    private final FileRecords fileRecords;
    private final ByteBuffer logHeaderBuffer = ByteBuffer.allocate(HEADER_SIZE_UP_TO_MAGIC);
    private int position;

    /**
     * Create a new log input stream over the FileChannel
     *
     * @param records Underlying FileRecords instance
     * @param start   Position in the file channel to start from
     * @param end     Position in the file channel not to read past
     */
    FileLogInputStream(FileRecords records,
                       int start,
                       int end) {
        this.fileRecords = records;
        this.position = start;
        this.end = end;
    }

    @Override
    public RecordBatchWithPosition nextBatch() throws IOException {
        // please note that we should keep the position in channel untouched
        // otherwise we can't let several FileLogInputStream share the same
        // FileRecords safely.
        final FileChannel channel = fileRecords.channel();
        final long invariantPos = channel.position();
        if (position >= end - HEADER_SIZE_UP_TO_MAGIC)
            return null;

        logHeaderBuffer.rewind();
        readFullyOrFail(channel, logHeaderBuffer, position, "log header");

        logHeaderBuffer.rewind();
        int size = logHeaderBuffer.getInt(SIZE_OFFSET);

        if (position > end - BASIC_HEADER_OVERHEAD - size)
            return null;

        final byte magic = logHeaderBuffer.get(MAGIC_OFFSET);
        if (magic != CURRENT_MAGIC_VALUE) {
            throw new BadRecordException("invalid MAGIC. expect: " + CURRENT_MAGIC_VALUE + ", actual: " + magic);
        }

        final DefaultRecordBatch batch = loadBatch(size + BASIC_HEADER_OVERHEAD);
        final int pos = position;
        position += batch.totalBatchSize();
        assert invariantPos == channel.position() : "expect:" + invariantPos + " actual:" + channel.position();
        return new RecordBatchWithPosition(batch, pos);
    }

    private DefaultRecordBatch loadBatch(int totalBatchSize) throws IOException {
        final FileChannel channel = fileRecords.channel();
        final ByteBuffer buffer = ByteBuffer.allocate(totalBatchSize);
        readFullyOrFail(channel, buffer, position, "load batch");
        buffer.rewind();
        return new DefaultRecordBatch(buffer);
    }

    public static class RecordBatchWithPosition implements RecordBatch {
        private DefaultRecordBatch recordBatch;
        private int position;

        RecordBatchWithPosition(DefaultRecordBatch recordBatch, int pos) {
            this.recordBatch = recordBatch;
            this.position = pos;
        }

        public int position() {
            return position;
        }

        RecordBatch recordBatch() {
            return recordBatch;
        }

        @Override
        public byte magic() {
            return recordBatch.magic();
        }

        @Override
        public long baseId() {
            return recordBatch.baseId();
        }

        @Override
        public long lastIdDelta() {
            return recordBatch.lastIdDelta();
        }

        @Override
        public long lastId() {
            return recordBatch.lastId();
        }

        @Override
        public long nextId() {
            return recordBatch.nextId();
        }

        @Override
        public int totalBatchSize() {
            return recordBatch.totalBatchSize();
        }

        @Override
        public int count() {
            return recordBatch.count();
        }

        @Override
        public long checksum() {
            return recordBatch.checksum();
        }

        @Override
        public boolean isValid() {
            return recordBatch.isValid();
        }

        @Override
        public Iterator<Record> iterator() {
            return recordBatch.iterator();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final RecordBatchWithPosition records = (RecordBatchWithPosition) o;
            return position == records.position &&
                    recordBatch.equals(records.recordBatch);
        }

        @Override
        public int hashCode() {
            return Objects.hash(recordBatch, position);
        }

        @Override
        public String toString() {
            return "RecordBatch{" +
                    "magic=" + magic() +
                    ", position=" + position() +
                    ", id=[" + baseId() + ", " + lastId() + "]" +
                    ", crc=" + checksum() +
                    '}';
        }
    }
}
