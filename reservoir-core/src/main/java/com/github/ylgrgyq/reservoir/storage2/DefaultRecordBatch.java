package com.github.ylgrgyq.reservoir.storage2;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.*;

import static com.github.ylgrgyq.reservoir.storage2.RecordsHeaderConstants.*;

/**
 * RecordBatch implementation. The schema is given below:
 * <p>
 * BaseId:  Int64
 * Length: Int32
 * Magic: Int8
 * CRC: Uint32
 * Attributes: Int16
 * LastIdDelta: Int32
 * Records: [Record]
 * </p>
 * Please note that the Length field only includes Magic, CRC, Attributes, LastIdDelta, Records fields.
 * The size of the Length field itself and the size of BaseId is not added to Length.
 * </p>
 * On the contrary, the {@link DefaultRecordBatch#totalBatchSize()} is the total
 * size of the DefaultRecordBatch in bytes, which includes the RecordBatch header and all the Record
 * contained in this batch.
 */
class DefaultRecordBatch implements RecordBatch {
    static final int CRC_OFFSET = HEADER_SIZE_UP_TO_MAGIC;
    static final int CRC_LENGTH = 4;
    static final int ATTRIBUTES_OFFSET = CRC_OFFSET + CRC_LENGTH;
    static final int ATTRIBUTE_LENGTH = 2;
    static final int LAST_ID_DELTA_OFFSET = ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH;
    static final int LAST_ID_DELTA_LENGTH = 4;
    static final int RECORDS_COUNT_OFFSET = LAST_ID_DELTA_OFFSET + LAST_ID_DELTA_LENGTH;
    static final int RECORDS_COUNT_LENGTH = 4;
    static final int RECORDS_OFFSET = RECORDS_COUNT_OFFSET + RECORDS_COUNT_LENGTH;
    static final int DEFAULT_RECORD_BATCH_HEADER_OVERHEAD = RECORDS_OFFSET;

    static void writeHeader(ByteBuffer buffer,
                            long baseId,
                            int lastIdDelta,
                            int sizeInBytes,
                            byte magic,
                            int recordsCount) {
        int position = buffer.position();
        buffer.putLong(position + BASE_ID_OFFSET, baseId);
        buffer.putInt(position + SIZE_OFFSET, sizeInBytes - BASIC_HEADER_OVERHEAD);
        buffer.put(position + MAGIC_OFFSET, magic);
        buffer.putShort(position + ATTRIBUTES_OFFSET, (short) 0);
        buffer.putInt(position + LAST_ID_DELTA_OFFSET, lastIdDelta);
        buffer.putInt(position + RECORDS_COUNT_OFFSET, recordsCount);
        // crc contains all the fields after CRC
        long crc = Crc32C.compute(buffer, ATTRIBUTES_OFFSET, sizeInBytes - ATTRIBUTES_OFFSET);
        buffer.putInt(position + CRC_OFFSET, (int) crc);
        buffer.position(position + DEFAULT_RECORD_BATCH_HEADER_OVERHEAD);
    }

    static int estimateTotalBatchSize(List<ByteBuffer> batch) {
        int size = DEFAULT_RECORD_BATCH_HEADER_OVERHEAD;

        int idDelta = 0;
        for (ByteBuffer value : batch) {
            size += DefaultRecord.calculateTotalRecordSize(idDelta++, value);
        }
        return size;
    }

    private final ByteBuffer buffer;

    public DefaultRecordBatch(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public byte magic() {
        return buffer.get(MAGIC_OFFSET);
    }

    @Override
    public long baseId() {
        return buffer.getLong(BASE_ID_OFFSET);
    }

    @Override
    public long lastIdDelta() {
        return buffer.getInt(LAST_ID_DELTA_OFFSET);
    }

    @Override
    public long lastId() {
        return baseId() + lastIdDelta();
    }

    @Override
    public long nextId() {
        return lastId() + 1L;
    }

    @Override
    public int totalBatchSize() {
        return BASIC_HEADER_OVERHEAD + buffer.getInt(SIZE_OFFSET);
    }

    @Override
    public int count() {
        return buffer.getInt(RECORDS_COUNT_OFFSET);
    }

    @Override
    public long checksum() {
        return ByteUtils.readUnsignedInt(buffer, CRC_OFFSET);
    }

    @Override
    public boolean isValid() {
        return totalBatchSize() >= DEFAULT_RECORD_BATCH_HEADER_OVERHEAD && checksum() == computeChecksum();
    }

    private long computeChecksum() {
        return Crc32C.compute(buffer, ATTRIBUTES_OFFSET, buffer.limit() - ATTRIBUTES_OFFSET);
    }

    @Override
    public Iterator<Record> iterator() {
        if (count() == 0) {
            return Collections.emptyIterator();
        }

        final ByteBuffer buffer = this.buffer.duplicate();
        buffer.position(RECORDS_OFFSET);

        return new RecordIterator() {
            @Override
            protected Record readNext(long baseId) {
                try {
                    return DefaultRecord.readFrom(buffer, baseId);
                } catch (BufferUnderflowException e) {
                    throw new BadRecordException("Incorrect declared batch size", e);
                }
            }

            @Override
            protected boolean ensureNoneRemaining() {
                return !buffer.hasRemaining();
            }
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final DefaultRecordBatch batch = (DefaultRecordBatch) o;
        return buffer.equals(batch.buffer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(buffer);
    }

    @Override
    public String toString() {
        return "RecordBatch{" +
                "magic=" + magic() +
                ", id=[" + baseId() + ", " + lastId() + "]" +
                ", crc=" + checksum() +
                '}';
    }

    private abstract class RecordIterator implements Iterator<Record> {
        private final long baseId;
        private final int numRecords;
        private int readRecordIndex = 0;

        RecordIterator() {
            this.baseId = baseId();
            final int numRecords = count();
            if (numRecords < 0)
                throw new BadRecordException("Found invalid record count: " + numRecords + " with magic: " +
                        magic() + " in batch");
            this.numRecords = numRecords;
        }

        @Override
        public boolean hasNext() {
            return readRecordIndex < numRecords;
        }

        @Override
        public Record next() {
            if (readRecordIndex >= numRecords)
                throw new NoSuchElementException("total records: " + numRecords + " current index: " + readRecordIndex);

            readRecordIndex++;
            final Record rec = readNext(baseId);
            if (readRecordIndex == numRecords) {
                // Validate that the actual size of the batch is equal to declared size
                // by checking that after reading declared number of items, there no items left
                // (overflow case, i.e. reading past buffer end is checked elsewhere).
                if (!ensureNoneRemaining())
                    throw new BadRecordException("Incorrect declared batch size: " + numRecords +
                            ", records still has remaining in batch");
            }
            return rec;
        }

        protected abstract Record readNext(long baseId);

        protected abstract boolean ensureNoneRemaining();
    }
}
