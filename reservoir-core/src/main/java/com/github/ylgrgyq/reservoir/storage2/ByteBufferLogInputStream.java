package com.github.ylgrgyq.reservoir.storage2;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

import static com.github.ylgrgyq.reservoir.storage2.RecordsHeaderConstants.*;

/**
 * A byte buffer backed log input stream. This class avoids the need to copy records by returning
 * slices from the underlying byte buffer.
 */
final class ByteBufferLogInputStream implements LogInputStream<DefaultRecordBatch> {
    private final ByteBuffer buffer;
    private final int maxMessageSize;

    ByteBufferLogInputStream(ByteBuffer buffer, int maxMessageSize) {
        this.buffer = buffer;
        this.maxMessageSize = maxMessageSize;
    }

    @Override
    public DefaultRecordBatch nextBatch() {
        int remaining = buffer.remaining();

        Integer batchSize = nextBatchSize();
        if (batchSize == null || remaining < batchSize)
            return null;

        ByteBuffer batchSlice = buffer.slice();
        batchSlice.limit(batchSize);
        buffer.position(buffer.position() + batchSize);

        return new DefaultRecordBatch(batchSlice);
    }

    /**
     * Validates the header of the next batch and returns batch size.
     *
     * @return next batch size including BASIC_HEADER_OVERHEAD if buffer contains header up to
     * magic byte, null otherwise
     * @throws BadRecordException if record size or magic is invalid
     */
    @Nullable
    private Integer nextBatchSize() throws BadRecordException {
        int remaining = buffer.remaining();
        if (remaining < BASIC_HEADER_OVERHEAD)
            return null;
        int recordSize = buffer.getInt(buffer.position() + SIZE_OFFSET);
        if (recordSize > maxMessageSize)
            throw new BadRecordException(String.format("Record size %d exceeds the largest allowable message size (%d).",
                    recordSize, maxMessageSize));

        if (remaining < HEADER_SIZE_UP_TO_MAGIC)
            return null;

        byte magic = buffer.get(buffer.position() + MAGIC_OFFSET);
        if (magic != CURRENT_MAGIC_VALUE)
            throw new BadRecordException("Invalid magic found in record: " + magic);

        return recordSize + BASIC_HEADER_OVERHEAD;
    }
}
