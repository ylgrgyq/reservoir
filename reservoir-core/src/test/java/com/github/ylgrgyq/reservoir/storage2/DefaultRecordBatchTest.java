package com.github.ylgrgyq.reservoir.storage2;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static com.github.ylgrgyq.reservoir.storage2.RecordsHeaderConstants.BASIC_HEADER_OVERHEAD;
import static com.github.ylgrgyq.reservoir.storage2.RecordsHeaderConstants.SIZE_OFFSET;
import static com.github.ylgrgyq.reservoir.storage2.StorageTestingUtils.generateTestingBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DefaultRecordBatchTest {
    private static final long defaultBaseId = 10101;
    private static final byte magic = RecordsHeaderConstants.CURRENT_MAGIC_VALUE;

    @Test
    public void testWriteEmptyHeader() {
        final ByteBuffer buffer = ByteBuffer.allocate(DefaultRecordBatch.estimateTotalBatchSize(Collections.emptyList()));
        final int lastIdDelta = 0;
        final int recordsCount = 0;

        DefaultRecordBatch.writeHeader(buffer,
                defaultBaseId,
                lastIdDelta,
                DefaultRecordBatch.DEFAULT_RECORD_BATCH_HEADER_OVERHEAD,
                magic,
                recordsCount);
        buffer.flip();

        final DefaultRecordBatch batch = new DefaultRecordBatch(buffer);
        assertThat(batch.baseId()).isEqualTo(defaultBaseId);
        assertThat(batch.count()).isZero();
        assertThat(batch.lastId()).isEqualTo(defaultBaseId);
        assertThat(batch.magic()).isEqualTo(RecordsHeaderConstants.CURRENT_MAGIC_VALUE);
        assertThat(batch.checksum()).isEqualTo(3822973035L);
        assertThat(batch.lastIdDelta()).isZero();
        assertThat(batch.totalBatchSize()).isEqualTo(DefaultRecordBatch.DEFAULT_RECORD_BATCH_HEADER_OVERHEAD);
        assertThat(batch.isValid()).isTrue();
        assertThat(batch.nextId()).isEqualTo(defaultBaseId + 1L);
        assertThat(batch.iterator()).isExhausted();
    }

    @Test
    public void testNonEmptyRecordBatch() throws IOException {
        final List<ByteBuffer> batch = generateTestingBytes(100);
        final MemoryRecords records = new MemoryRecords(defaultBaseId, batch);

        assertThat(records.batches()).hasSize(1);

        for (RecordBatch recordBatch : records.batches()) {
            assertThat(recordBatch.baseId()).isEqualTo(defaultBaseId);
            assertThat(recordBatch.count()).isEqualTo(batch.size());
            assertThat(recordBatch.magic()).isEqualTo(RecordsHeaderConstants.CURRENT_MAGIC_VALUE);
            assertThat(recordBatch.lastId()).isEqualTo(defaultBaseId + batch.size() - 1);
            assertThat(recordBatch.lastIdDelta()).isEqualTo(batch.size() - 1);
            assertThat(recordBatch.nextId()).isEqualTo(recordBatch.lastId() + 1);
            assertThat(recordBatch.isValid()).isTrue();
            assertThat(recordBatch.checksum()).isEqualTo(1547212387L);
            assertThat(recordBatch.totalBatchSize()).isEqualTo(DefaultRecordBatch.estimateTotalBatchSize(batch));

            int recordIndex = 0;
            for (Record record : recordBatch) {
                assertThat(record.id()).isEqualTo(defaultBaseId + recordIndex);
                assertThat(record.value()).isEqualTo(batch.get(recordIndex));
                assertThat(record.valueSize()).isEqualTo(batch.get(recordIndex).limit());
                recordIndex++;
            }
            assertThat(recordBatch.count()).isEqualTo(recordIndex);
        }
    }

    @Test
    public void testNegativeRecordCount() {
        final ByteBuffer buffer = ByteBuffer.allocate(DefaultRecordBatch.estimateTotalBatchSize(Collections.emptyList()));
        final int lastIdDelta = 1;

        DefaultRecordBatch.writeHeader(buffer,
                defaultBaseId,
                lastIdDelta,
                DefaultRecordBatch.DEFAULT_RECORD_BATCH_HEADER_OVERHEAD,
                magic,
                -1);
        buffer.flip();

        final DefaultRecordBatch batch = new DefaultRecordBatch(buffer);
        assertThatThrownBy(() -> batch.iterator().next())
                .isInstanceOf(BadRecordException.class)
                .hasMessage("Found invalid record count: -1 with magic: 0 in batch");
    }

    @Test
    public void testNoSuchElementExceptionAfterDrainRecord() throws IOException {
        final List<ByteBuffer> batch = generateTestingBytes(1);
        final MemoryRecords records = new MemoryRecords(defaultBaseId, batch);
        final RecordBatch recordBatch = records.batches().iterator().next();
        final Iterator<Record> itr = recordBatch.iterator();
        assertThat(itr.next().value()).isEqualTo(batch.get(0));
        assertThatThrownBy(itr::next)
                .isInstanceOf(NoSuchElementException.class)
                .hasMessageContaining("total records: 1 current index: 1");
    }

    @Test
    public void testBufferUnderflow() {
        final ByteBuffer buffer = ByteBuffer.allocate(DefaultRecordBatch.estimateTotalBatchSize(Collections.emptyList()));
        final int lastIdDelta = 1;
        final int recordsCount = 1;

        DefaultRecordBatch.writeHeader(buffer,
                defaultBaseId,
                lastIdDelta,
                DefaultRecordBatch.DEFAULT_RECORD_BATCH_HEADER_OVERHEAD,
                magic,
                recordsCount);
        buffer.flip();

        final DefaultRecordBatch batch = new DefaultRecordBatch(buffer);
        assertThatThrownBy(() -> batch.iterator().next())
                .isInstanceOf(BadRecordException.class)
                .hasMessage("Incorrect declared batch size");
    }

    @Test
    public void testUnderflowInvalidRecordBatch() {
        final ByteBuffer buffer = ByteBuffer.allocate(DefaultRecordBatch.estimateTotalBatchSize(Collections.emptyList()));
        final int lastIdDelta = 0;
        final int recordsCount = 0;

        DefaultRecordBatch.writeHeader(buffer,
                defaultBaseId,
                lastIdDelta,
                DefaultRecordBatch.DEFAULT_RECORD_BATCH_HEADER_OVERHEAD,
                magic,
                recordsCount);
        buffer.flip();
        buffer.putInt(SIZE_OFFSET, DefaultRecordBatch.DEFAULT_RECORD_BATCH_HEADER_OVERHEAD - BASIC_HEADER_OVERHEAD - 1);

        final DefaultRecordBatch batch = new DefaultRecordBatch(buffer);
        assertThat(batch.isValid()).isFalse();
    }

    @Test
    public void testInvalidChecksumRecordBatch() {
        final ByteBuffer buffer = ByteBuffer.allocate(DefaultRecordBatch.estimateTotalBatchSize(Collections.emptyList()));
        final int lastIdDelta = 0;
        final int recordsCount = 0;

        DefaultRecordBatch.writeHeader(buffer,
                defaultBaseId,
                lastIdDelta,
                DefaultRecordBatch.DEFAULT_RECORD_BATCH_HEADER_OVERHEAD,
                magic,
                recordsCount);
        buffer.flip();
        buffer.putInt(DefaultRecordBatch.CRC_OFFSET, 101);

        final DefaultRecordBatch batch = new DefaultRecordBatch(buffer);
        assertThat(batch.isValid()).isFalse();
    }
}