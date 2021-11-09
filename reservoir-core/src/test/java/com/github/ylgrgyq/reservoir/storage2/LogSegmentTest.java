package com.github.ylgrgyq.reservoir.storage2;

import com.github.ylgrgyq.reservoir.FileUtils;
import com.github.ylgrgyq.reservoir.StorageException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.github.ylgrgyq.reservoir.storage2.RecordsHeaderConstants.MAGIC_OFFSET;
import static com.github.ylgrgyq.reservoir.storage2.StorageTestingUtils.generateTestingBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class LogSegmentTest {
    private final long testingStartId = 1010101;
    private Path testingDirPath;
    private Path segmentFilePath;
    private LogSegment segment;

    @Before
    public void setUp() throws Exception {
        String tempDir = System.getProperty("java.io.tmpdir", "/tmp") +
                File.separator + "reservoir_test_" + System.nanoTime();
        testingDirPath = Paths.get(tempDir);
        FileUtils.forceMkdir(testingDirPath.toFile());
        segmentFilePath = testingDirPath.resolve("segment");
        segment = LogSegment.newSegment(segmentFilePath, testingStartId);
    }

    @After
    public void tearDown() throws Exception {
        segment.close();
    }

    @Test
    public void testCreateSegmentOnExitingFile() {
        assertThatThrownBy(() -> LogSegment.newSegment(segmentFilePath, testingStartId))
                .isInstanceOf(StorageException.class)
                .hasMessage("segment on path " + segmentFilePath + " is already exists");
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

        for (long i = testingStartId; i <= segment.lastId(); i++) {
            final FileRecords records = segment.records(i);
            assert records != null;

            int index = (int) (i - testingStartId);
            for (Record record : records.records()) {
                if (record.id() >= i) {
                    assertThat(record.id()).isEqualTo(testingStartId + index);
                    assertThat(record.value()).isEqualTo(data.get(index));
                    index++;
                }
            }
        }
    }

    @Test
    public void testReadFromIdSmallerThanLastId() throws Exception {
        final List<ByteBuffer> data = generateTestingBytes(10);
        segment.append(data);

        final FileRecords records = segment.records(ThreadLocalRandom.current().nextLong(Long.MIN_VALUE, segment.startId()));
        assert records != null;

        int index = 0;
        for (Record record : records.records()) {
            assertThat(record.id()).isEqualTo(testingStartId + index);
            assertThat(record.value()).isEqualTo(data.get(index));
            index++;
        }
    }

    @Test
    public void testReadFromIdGreaterThanLastId() throws Exception {
        final List<ByteBuffer> data = generateTestingBytes(10);
        segment.append(data);

        assertThat(segment.records(segment.lastId() + 1)).isNull();
    }

    @Test
    public void testNewSegmentInitialLastId() {
        assertThat(segment.lastId()).isEqualTo(testingStartId - 1);
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
    public void testCreationTime() throws Exception {
        final BasicFileAttributes attr = Files.readAttributes(segmentFilePath, BasicFileAttributes.class);

        assertThat(segment.creationTime()).isEqualTo(attr.creationTime().toInstant());
    }

    @Test
    public void testClose() throws IOException {
        LogSegment closeSegment = LogSegment.newSegment(testingDirPath.resolve("close-segment"), testingStartId);
        FileRecords records = closeSegment.fileRecords();
        closeSegment.close();
        assertThat(records.channel().isOpen()).isFalse();
    }

    @Test
    public void recoverFromPath() throws Exception {
        final List<ByteBuffer> data = generateTestingBytes(15);
        segment.append(data);

        final LogSegment recoveredSegment = LogSegment.fromFilePath(segmentFilePath);
        assertThat(recoveredSegment).isEqualTo(segment);
    }

    @Test
    public void recoverEmptySegment() {
        assertThatThrownBy(() -> LogSegment.fromFilePath(segmentFilePath))
                .isInstanceOf(EmptySegmentException.class)
                .hasMessageContaining("found empty segment under path:");
    }

    @Test
    public void recoverFromBadSegmentFile() throws Exception {
        final List<ByteBuffer> data = generateTestingBytes(15);
        segment.append(data);

        final FileChannel channel = FileChannel.open(segmentFilePath, StandardOpenOption.WRITE);
        final ByteBuffer badMagic = ByteBuffer.allocate(1);
        channel.position(MAGIC_OFFSET);
        badMagic.put(0, (byte) 101);
        channel.write(badMagic);

        assertThatThrownBy(() -> LogSegment.fromFilePath(segmentFilePath))
                .isInstanceOf(StorageRuntimeException.class)
                .hasMessageContaining("recover from segment file: " + segmentFilePath + " failed");
    }
}