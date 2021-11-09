package com.github.ylgrgyq.reservoir.storage2;

import com.github.ylgrgyq.reservoir.FileUtils;
import com.github.ylgrgyq.reservoir.storage2.FileLogInputStream.RecordBatchWithPosition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.github.ylgrgyq.reservoir.storage2.StorageTestingUtils.generateMemoryRecords;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class FileRecordsTest {
    private final long defaultBaseId = 10000;
    private final int defaultBatchSize = 100;
    private final int defaultRecordsCount = 1000;
    private File tempFile;
    private FileRecords fileRecords;

    @Before
    public void setUp() throws Exception {
        final String tempDir = System.getProperty("java.io.tmpdir", "/tmp") +
                File.separator + "reservoir_test_" + System.nanoTime();
        FileUtils.forceMkdir(new File(tempDir));
        tempFile = new File(tempDir + File.separator + "file_records_test");
        fileRecords = FileRecords.open(tempFile);
    }

    @After
    public void tearDown() throws Exception {
        fileRecords.close();
    }

    @Test
    public void testWriteReadSingleMemoryRecords() throws IOException {
        final MemoryRecords records = generateMemoryRecords(defaultBaseId, defaultBatchSize);

        assertThat(fileRecords.append(records)).isEqualTo(records.lastId());
        assertThat(fileRecords.batches()).hasSize(1);
        assertThat(fileRecords.batches().iterator().next().recordBatch()).isEqualTo(records.batches().iterator().next());
    }

    @Test
    public void testWriteReadMultiMemoryRecords() throws IOException {
        final List<MemoryRecords> recordsList = prepareSomeRecords(defaultBaseId, defaultRecordsCount, defaultBatchSize);

        int index = 0;
        assertThat(fileRecords.batches()).hasSize(defaultRecordsCount);
        for (RecordBatchWithPosition batch : fileRecords.batches()) {
            assertThat(batch.recordBatch()).isEqualTo(recordsList.get(index++).batches().iterator().next());
        }
    }

    @Test
    public void testAppendToASlice() throws IOException {
        final List<MemoryRecords> recordsList = prepareSomeRecords(defaultBaseId, defaultRecordsCount, defaultBatchSize);
        final MemoryRecords records = generateMemoryRecords(recordsList.get(recordsList.size() - 1).lastId() + 1, 1);
        FileRecords slice = fileRecords.slice(10, 100);
        assertThatThrownBy(() -> slice.append(records))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("append is not allowed on a slice");
    }

    @Test
    public void testReadExistingFile() throws IOException {
        final List<MemoryRecords> recordsList = prepareSomeRecords(defaultBaseId, defaultRecordsCount, defaultBatchSize);

        try (FileRecords records = FileRecords.open(tempFile)) {
            int index = 0;
            assertThat(records.batches()).hasSize(defaultRecordsCount);
            for (RecordBatchWithPosition batch : records.batches()) {
                assertThat(batch.recordBatch()).isEqualTo(recordsList.get(index++).batches().iterator().next());
            }
        }
    }

    @Test
    public void testSlice() throws IOException {
        final List<MemoryRecords> recordsList = prepareSomeRecords(defaultBaseId, defaultRecordsCount, 50);

        for (int i = 0; i < recordsList.size() - 1; i++) {
            final Integer pos = fileRecords.searchRecordBatchStartPosForId(0, recordsList.get(i).lastId() + 1);
            assert (pos != null);
            FileRecords slice = fileRecords.slice(pos, fileRecords.totalSizeInBytes());

            int index = i + 1;
            assertThat(slice.batches()).hasSize(recordsList.size() - i - 1);
            for (RecordBatchWithPosition batch : slice.batches()) {
                assertThat(batch.recordBatch()).isEqualTo(recordsList.get(index++).batches().iterator().next());
            }
        }
    }

    @Test
    public void testNegativePosition() {
        assertThatThrownBy(() -> fileRecords.slice(-1, fileRecords.totalSizeInBytes()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("position: -1 (expected: >= 0)");
    }

    @Test
    public void testPositionGreaterThanTotalSize() throws IOException {
        prepareSomeRecords(101, 10, 10);
        assertThatThrownBy(() -> fileRecords.slice(fileRecords.totalSizeInBytes() + 1, fileRecords.totalSizeInBytes()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("position: 771 (expected between: [0, 770) )");
    }

    @Test
    public void testNegativeSize() {
        assertThatThrownBy(() -> fileRecords.slice(0, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("size: -1 (expected: >= 0)");
    }

    @Test
    public void testSearchForNonExistsId() throws IOException {
        final long targetId = ThreadLocalRandom.current().nextLong(defaultBaseId + defaultRecordsCount, Long.MAX_VALUE);
        final List<MemoryRecords> recordsList = prepareSomeRecords(defaultBaseId, defaultRecordsCount, defaultBatchSize);

        int pos = 0;
        for (MemoryRecords batch : recordsList) {
            assertThat(fileRecords.searchRecordBatchStartPosForId(pos, targetId)).isNull();
            pos += batch.totalSizeInBytes();
        }
    }

    @Test
    public void testSearchIdBeforeStartingPos() throws IOException {
        final List<MemoryRecords> recordsList = prepareSomeRecords(defaultBaseId, defaultRecordsCount, defaultBatchSize);

        long lastBaseId = defaultBaseId;
        int pos = 0;
        for (MemoryRecords batch : recordsList) {
            long targetId = ThreadLocalRandom.current().nextLong(0, lastBaseId);
            assertThat(fileRecords.searchRecordBatchStartPosForId(pos, targetId)).isEqualTo(pos);
            lastBaseId = batch.baseId();
            pos += batch.totalSizeInBytes();
        }
    }

    @Test
    public void testSearchForId() throws IOException {
        final List<MemoryRecords> recordsList = prepareSomeRecords(defaultBaseId, 10, defaultBatchSize);

        int searchStartPos = 0;
        for (int i = 0; i < recordsList.size(); ++i) {
            int expectPos = searchStartPos;
            for (int j = i + 1; j < recordsList.size(); ++j) {
                final MemoryRecords prevRecords = recordsList.get(j - 1);
                final MemoryRecords currentRecords = recordsList.get(j);
                final long targetId = ThreadLocalRandom.current().nextLong(prevRecords.baseId(), currentRecords.baseId());
                assertThat(fileRecords.searchRecordBatchStartPosForId(searchStartPos, targetId)).isEqualTo(expectPos);
                expectPos += prevRecords.totalSizeInBytes();
            }

            searchStartPos += recordsList.get(i).totalSizeInBytes();
        }
    }

    @Test
    public void testCloseFileRecords() throws IOException {
        final FileRecords records = FileRecords.open(tempFile);
        assertThat(records.channel().isOpen()).isTrue();
        records.close();
        assertThat(records.channel().isOpen()).isFalse();
    }

    @Test
    public void testCloseSliceFileRecords() throws IOException {
        prepareSomeRecords(defaultBaseId, defaultRecordsCount, defaultBatchSize);
        final FileRecords slice = fileRecords.slice(10, 100);
        assertThat(slice.channel().isOpen()).isTrue();
        slice.close();
        assertThat(slice.channel().isOpen()).isTrue();
    }

    private List<MemoryRecords> prepareSomeRecords(long baseId, int recordsCount, int batchSize) throws IOException {
        final List<MemoryRecords> recordsList = new ArrayList<>(recordsCount);
        for (int i = 0; i < recordsCount; i++) {
            final MemoryRecords records = generateMemoryRecords(baseId, batchSize);
            recordsList.add(records);
            fileRecords.append(records);
            baseId = records.lastId() + 1L;
        }
        return recordsList;
    }
}