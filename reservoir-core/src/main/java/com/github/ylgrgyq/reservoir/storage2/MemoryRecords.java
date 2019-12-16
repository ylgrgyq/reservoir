package com.github.ylgrgyq.reservoir.storage2;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;
import java.util.List;

import static com.github.ylgrgyq.reservoir.storage2.DefaultRecordBatch.DEFAULT_RECORD_BATCH_HEADER_OVERHEAD;

public final class MemoryRecords extends AbstractRecords {
    private final long baseId;
    private final Iterable<DefaultRecordBatch> batches = this::batchIterator;
    private final ByteBuffer buffer;
    private final int numOfRecords;

    MemoryRecords(long baseId, List<ByteBuffer> batch) throws IOException {
        this.baseId = baseId;

        final int bufferSize = DefaultRecordBatch.estimateTotalBatchSize(batch);
        final ByteBufferOutputStream outputStream = new ByteBufferOutputStream(bufferSize);
        outputStream.position(DEFAULT_RECORD_BATCH_HEADER_OVERHEAD);
        final DataOutputStream out = new DataOutputStream(outputStream);
        int idDelta = -1;
        for (ByteBuffer value : batch) {
            DefaultRecord.writeTo(out, ++idDelta, value);
        }
        out.flush();

        final ByteBuffer innerBuffer = outputStream.buffer();
        final int pos = innerBuffer.position();
        innerBuffer.position(0);
        DefaultRecordBatch.writeHeader(innerBuffer,
                baseId,
                idDelta,
                pos,
                RecordsHeaderConstants.CURRENT_MAGIC_VALUE,
                batch.size());

        innerBuffer.position(pos).flip();
        this.buffer = innerBuffer;
        this.numOfRecords = batch.size();
    }

    @Override
    public int totalSizeInBytes() {
        return buffer.limit();
    }

    @Override
    public Iterable<? extends RecordBatch> batches() {
        return batches;
    }

    /**
     * Get the id of the first record in this {@link MemoryRecords}.
     *
     * @return the first record (inclusive) id in this {@link MemoryRecords}
     */
    public long baseId() {return baseId;}

    /**
     * Get the id of the last record in this {@link MemoryRecords}.
     *
     * @return the last record (inclusive) id in this {@link MemoryRecords}
     */
    public long lastId() {
        return baseId + numOfRecords - 1;
    }

    public ByteBuffer buffer() {
        return buffer;
    }

    int writeFullyTo(GatheringByteChannel channel) throws IOException {
        buffer.mark();
        int written = 0;
        while (written < totalSizeInBytes()) {
            written += channel.write(buffer);
        }
        buffer.reset();
        return written;
    }

    private Iterator<DefaultRecordBatch> batchIterator() {
        return new RecordBatchIterator<>(new ByteBufferLogInputStream(buffer.duplicate(), Integer.MAX_VALUE));
    }

}
