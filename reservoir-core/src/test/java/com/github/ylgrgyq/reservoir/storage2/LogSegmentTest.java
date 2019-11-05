package com.github.ylgrgyq.reservoir.storage2;

import com.github.ylgrgyq.reservoir.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.github.ylgrgyq.reservoir.storage2.StorageTestingUtils.generateTestingBytes;
import static org.assertj.core.api.Assertions.assertThat;

public class LogSegmentTest {
    private final long testingStartId = 1010101;
    private Path testingDirPath;
    private LogSegment segment;

    @Before
    public void setUp() throws Exception {
        String tempDir = System.getProperty("java.io.tmpdir", "/tmp") +
                File.separator + "reservoir_test_" + System.nanoTime();
        testingDirPath = Paths.get(tempDir);
        FileUtils.forceMkdir(testingDirPath.toFile());
        segment = LogSegment.newSegment(testingDirPath.resolve("segment"), testingStartId);
    }

    @After
    public void tearDown() throws Exception {
        segment.close();
    }

    @Test
    public void testStartId() {
        assertThat(segment.startId()).isEqualTo(testingStartId);
    }

    @Test
    public void testReadFromEmptySegments() throws IOException {
        assertThat(segment.records(testingStartId)).isNull();
    }

    @Test
    public void testReadAndWrite() throws IOException {
        final List<ByteBuffer> data = generateTestingBytes(10);
        segment.append(data);

        final FileRecords records = segment.records(testingStartId);
        assert records != null;

        int index = 0;
        for (Record record : records.records()) {
            assertThat(record.id()).isEqualTo(testingStartId + index);
            assertThat(record.value()).isEqualTo(data.get(index));
            index++;
        }
    }

    @Test
    public void testNewSegmentInitialLastId() {
        assertThat(segment.lastId()).isEqualTo(testingStartId);
    }

    @Test
    public void testLastId() throws IOException {
        final List<ByteBuffer> data = generateTestingBytes(10);
        segment.append(data);

        assertThat(segment.lastId()).isEqualTo(testingStartId + data.size() - 1);
    }

    @Test
    public void testSize() throws IOException {
        final List<ByteBuffer> data = generateTestingBytes(10);
        segment.append(data);
        assertThat(segment.size()).isEqualTo(segment.fileRecords().totalSizeInBytes());
    }

    @Test
    public void testClose() throws IOException {
        LogSegment closeSegment = LogSegment.newSegment(testingDirPath.resolve("close-segment"), testingStartId);
        FileRecords records = closeSegment.fileRecords();
        closeSegment.close();
        assertThat(records.channel().isOpen()).isFalse();
    }
}