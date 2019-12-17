package com.github.ylgrgyq.reservoir.storage2;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static com.github.ylgrgyq.reservoir.storage2.StorageTestingUtils.generateTestingBytes;
import static org.assertj.core.api.Assertions.assertThat;

public class MemoryRecordsTest {
    @Test
    public void testBatches() throws IOException {
        final long baseId = 10000;
        final List<ByteBuffer> batch = generateTestingBytes(100);
        final MemoryRecords records = new MemoryRecords(baseId, batch);

        assertThat(records.batches()).hasSize(1);
        assertThat(records.lastId()).isEqualTo(baseId + batch.size() - 1);

        for (RecordBatch recordBatch : records.batches()) {
            assertThat(recordBatch.baseId()).isEqualTo(baseId);
            assertThat(recordBatch.count()).isEqualTo(batch.size());
            assertThat(recordBatch.magic()).isEqualTo(RecordsHeaderConstants.CURRENT_MAGIC_VALUE);
            assertThat(recordBatch.lastId()).isEqualTo(baseId + batch.size() - 1);
            assertThat(recordBatch.lastIdDelta()).isEqualTo(batch.size() - 1);
            assertThat(recordBatch.nextId()).isEqualTo(recordBatch.lastId() + 1);
            assertThat(recordBatch.totalBatchSize()).isEqualTo(records.totalSizeInBytes());

            int recordIndex = 0;
            for (Record record : recordBatch) {
                assertThat(record.id()).isEqualTo(baseId + recordIndex);
                assertThat(record.value()).isEqualTo(batch.get(recordIndex));
                assertThat(record.valueSize()).isEqualTo(batch.get(recordIndex).limit());
                recordIndex++;
            }
            assertThat(recordBatch.count()).isEqualTo(recordIndex);
        }
    }

    @Test
    public void testRecords() throws IOException {
        final long baseId = 20000;
        final List<ByteBuffer> batch = generateTestingBytes(200);
        final MemoryRecords records = new MemoryRecords(baseId, batch);

        int recordIndex = 0;
        for (Record record : records.records()) {
            assertThat(record.id()).isEqualTo(baseId + recordIndex);
            assertThat(record.value()).isEqualTo(batch.get(recordIndex));
            assertThat(record.valueSize()).isEqualTo(batch.get(recordIndex).limit());
            recordIndex++;
        }
    }
}