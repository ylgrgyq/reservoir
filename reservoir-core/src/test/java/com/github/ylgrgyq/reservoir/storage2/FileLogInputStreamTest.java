package com.github.ylgrgyq.reservoir.storage2;

import com.github.ylgrgyq.reservoir.FileUtils;
import com.github.ylgrgyq.reservoir.storage2.FileLogInputStream.RecordBatchWithPosition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.github.ylgrgyq.reservoir.storage2.RecordsHeaderConstants.HEADER_SIZE_UP_TO_MAGIC;
import static com.github.ylgrgyq.reservoir.storage2.RecordsHeaderConstants.MAGIC_OFFSET;
import static com.github.ylgrgyq.reservoir.storage2.StorageTestingUtils.generateMemoryRecords;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class FileLogInputStreamTest {
    private final long defaultBaseId = 10000;
    private final int defaultBatchSize = 100;
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
    public void testWriteReadSingleRecords() throws IOException {
        final MemoryRecords records = generateMemoryRecords(defaultBaseId, defaultBatchSize);
        final RecordBatch batch = records.batches().iterator().next();
        assertThat(fileRecords.append(records)).isEqualTo(records.lastId());
        final FileLogInputStream in = new FileLogInputStream(fileRecords, 0, fileRecords.totalSizeInBytes());
        final RecordBatchWithPosition record = in.nextBatch();
        assert record != null;
        assertThat(record.position()).isZero();
        assertThat(record.baseId()).isEqualTo(defaultBaseId);
        assertThat(record.recordBatch()).isEqualTo(batch);
        assertThat(record.lastId()).isEqualTo(records.lastId());
        assertThat(record.lastIdDelta()).isEqualTo(records.lastId() - defaultBaseId);
        assertThat(record.nextId()).isEqualTo(defaultBaseId + defaultBatchSize);
        assertThat(record.count()).isEqualTo(defaultBatchSize);
        assertThat(record.checksum()).isEqualTo(batch.checksum());
        assertThat(record.totalBatchSize()).isEqualTo(fileRecords.channel().size()).isEqualTo(records.totalSizeInBytes());
        assertThat(record.magic()).isEqualTo(RecordsHeaderConstants.CURRENT_MAGIC_VALUE);
        assertThat(record.isValid()).isTrue();
    }

    @Test
    public void testReadUnfinishedHeaderRecords() throws IOException {
        final MemoryRecords records = generateMemoryRecords(defaultBaseId, defaultBatchSize);
        assertThat(fileRecords.append(records)).isEqualTo(records.lastId());
        final FileLogInputStream in = new FileLogInputStream(fileRecords, 0, HEADER_SIZE_UP_TO_MAGIC);
        assertThat(in.nextBatch()).isNull();
    }

    @Test
    public void testReadUnfinishedRecords() throws IOException {
        final MemoryRecords records = generateMemoryRecords(defaultBaseId, defaultBatchSize);
        assertThat(fileRecords.append(records)).isEqualTo(records.lastId());
        final FileLogInputStream in = new FileLogInputStream(fileRecords, 0, fileRecords.totalSizeInBytes() - 1);
        assertThat(in.nextBatch()).isNull();
    }

    @Test
    public void testUnmatchedMagic() throws IOException {
        final MemoryRecords records = generateMemoryRecords(defaultBaseId, defaultBatchSize);
        assertThat(fileRecords.append(records)).isEqualTo(records.lastId());
        final FileLogInputStream in = new FileLogInputStream(fileRecords, 0, fileRecords.totalSizeInBytes());
        final FileChannel channel = fileRecords.channel();
        final ByteBuffer badMagic = ByteBuffer.allocate(1);
        channel.position(MAGIC_OFFSET);
        badMagic.put(0, (byte) 101);
        channel.write(badMagic);

        assertThatThrownBy(in::nextBatch).isInstanceOf(BadRecordException.class)
                .hasMessage("invalid MAGIC. expect: 0, actual: 101");
    }

    @Test
    public void testHeaderEof() throws IOException {
        final MemoryRecords records = generateMemoryRecords(defaultBaseId, defaultBatchSize);
        assertThat(fileRecords.append(records)).isEqualTo(records.lastId());
        final FileLogInputStream in = new FileLogInputStream(fileRecords, 0, fileRecords.totalSizeInBytes());
        fileRecords.channel().truncate(HEADER_SIZE_UP_TO_MAGIC - 1);
        assertThatThrownBy(in::nextBatch)
                .isInstanceOf(EOFException.class)
                .hasMessageContaining("Failed to read `log header` from file channel");
    }

    @Test
    public void testLoadBatchEof() throws IOException {
        final MemoryRecords records = generateMemoryRecords(defaultBaseId, defaultBatchSize);
        assertThat(fileRecords.append(records)).isEqualTo(records.lastId());
        final FileLogInputStream in = new FileLogInputStream(fileRecords, 0, fileRecords.totalSizeInBytes());
        fileRecords.channel().truncate(fileRecords.totalSizeInBytes() - 1);
        assertThatThrownBy(in::nextBatch)
                .isInstanceOf(EOFException.class)
                .hasMessageContaining("Failed to read `load batch` from file channel");
    }
}