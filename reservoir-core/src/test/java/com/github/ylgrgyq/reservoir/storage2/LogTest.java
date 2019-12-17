package com.github.ylgrgyq.reservoir.storage2;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static com.github.ylgrgyq.reservoir.storage2.StorageTestingUtils.generateTestingBytes;
import static java.util.Comparator.comparing;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class LogTest {
    private static final String logName = "TestingLogName";
    private static final int maxSegmentSize = 1024;
    private Path testingDirPath;
    private Log testingLog;

    @Before
    public void setUp() throws Exception {
        String tempDir = System.getProperty("java.io.tmpdir", "/tmp") +
                File.separator + "reservoir_test_" + System.nanoTime();
        testingDirPath = Paths.get(tempDir);
        testingLog = new Log(testingDirPath, logName, maxSegmentSize);
    }

    @After
    public void tearDown() throws Exception {
        testingLog.close();
    }

    @Test
    public void testBasicReadAndWrite() throws Exception {
        final List<ByteBuffer> data = prepareDataInLog(1, 10);
        testReadLog(data, testingLog);
    }

    @Test
    public void testReadAndWriteWithMultiSegments() throws IOException {
        final List<ByteBuffer> data = prepareDataInLog(10, 100);
        testReadLog(data, testingLog);
        assertThat(testingLog.size()).isGreaterThan(1);
    }

    @Test
    public void testReadFromEmptyLog() throws Exception {
        assertThat(testingLog.read(100)).isNull();
    }

    @Test
    public void testReadBeforeFirstId() throws Exception {
        final List<ByteBuffer> data = prepareDataInLog(1, 10);
        final FileRecords records = testingLog.read(-1);
        assert records != null;

        int index = 0;
        for (Record record : records.records()) {
            assertThat(record.id()).isEqualTo(index);
            assertThat(record.value()).isEqualTo(data.get(index));
            index++;
        }
    }

    @Test
    public void getLastIdFromEmptyLog() {
        assertThat(testingLog.lastId()).isEqualTo(-1);
    }

    @Test
    public void testRecovery() throws Exception {
        final List<ByteBuffer> data = prepareDataInLog(10, 100);
        final Log recoveredLog = new Log(testingDirPath, logName, maxSegmentSize);
        testReadLog(data, recoveredLog);
        assertThat(recoveredLog.size()).isEqualTo(testingLog.size());
    }

    @Test
    public void testRecoverEmptySegment() throws Exception {
        final int batchCount = 10;
        final int batchSize = 100;
        final List<ByteBuffer> data = prepareDataInLog(batchCount, batchSize);
        final long emptySegmentStartId = batchCount * batchSize;
        final Path emptySegmentPath = testingDirPath.resolve(logName).resolve(SegmentFile.fileName(emptySegmentStartId));
        LogSegment.newSegment(emptySegmentPath, emptySegmentStartId);

        assertThat(emptySegmentPath.toFile().exists()).isTrue();

        final Log recoveredLog = new Log(testingDirPath, logName, maxSegmentSize);
        testReadLog(data, recoveredLog);
        assertThat(recoveredLog.size()).isEqualTo(testingLog.size());
        assertThat(emptySegmentPath.toFile().exists()).isFalse();
    }

    @Test
    public void testRecoverNonConsecutiveSegments() throws Exception {
        prepareDataInLog(10, 100);
        final List<File> segmentFiles = getOrderedSegmentFiles();

        // remove one of the segments lies in the middle of all segments to trigger the exception
        final int removeIndex = ThreadLocalRandom.current().nextInt(1, segmentFiles.size() - 1);
        Files.delete(segmentFiles.get(removeIndex).toPath());

        assertThatThrownBy(() -> new Log(testingDirPath, logName, maxSegmentSize))
                .hasMessageContaining("non-consecutive segments found under log: " + logName)
                .isInstanceOf(InvalidSegmentException.class);
    }

    @Test
    public void testPurgeSegments() throws Exception {
        prepareDataInLog(10, 100);
        final List<File> segmentFiles = getOrderedSegmentFiles();
        long startId = parseStartIdFromFileName(segmentFiles.get(segmentFiles.size() / 2).getName());
        int purgedCount = testingLog.purgeSegments(segment -> segment.startId() <= startId);
        assertThat(purgedCount).isPositive();

        final List<File> segmentFilesAfterPurge = getOrderedSegmentFiles();
        assertThat(segmentFilesAfterPurge.size()).isEqualTo(segmentFiles.size() - purgedCount);
        assertThat(segmentFilesAfterPurge
                .stream()
                .filter(f -> parseStartIdFromFileName(f.getName()) <= startId)
                .findAny())
                .isEmpty();
    }

    @Test
    public void testSkipPurgeWhenOnlyOneSegmentRemain() throws Exception {
        prepareDataInLog(10, 100);
        final List<File> segmentFiles = getOrderedSegmentFiles();
        int purgedCount = testingLog.purgeSegments(segment -> true);
        assertThat(purgedCount).isEqualTo(segmentFiles.size() - 1);

        final List<File> segmentFilesAfterPurge = getOrderedSegmentFiles();
        assertThat(segmentFilesAfterPurge.size()).isEqualTo(1);
        assertThat(segmentFilesAfterPurge).isEqualTo(segmentFiles.subList(segmentFiles.size() - 1, segmentFiles.size()));
    }

    private List<ByteBuffer> prepareDataInLog(int batchCount, int batchSize) throws IOException {
        final List<ByteBuffer> ret = new ArrayList<>();
        for (int i = 0; i < batchCount; ++i) {
            final List<ByteBuffer> data = generateTestingBytes(batchSize);
            testingLog.append(data);
            ret.addAll(data);
        }
        assertThat(testingLog.lastId()).isEqualTo(batchCount * batchSize - 1);
        return ret;
    }

    private void testReadLog(List<ByteBuffer> data, Log testingLog) throws IOException {
        for (long i = 0; i < data.size(); i++) {
            final FileRecords records = testingLog.read(i);
            assert records != null;

            int index = (int) i;
            for (Record record : records.records()) {
                if (record.id() >= i) {
                    assertThat(record.id()).isEqualTo(index);
                    assertThat(record.value()).isEqualTo(data.get(index));
                    index++;
                }
            }
        }
    }

    private List<File> getOrderedSegmentFiles() {
        final File[] files = testingDirPath.resolve(logName).toFile().listFiles();
        assert files != null;
        return Arrays.stream(files)
                .filter(f -> FileType.Segment.match(f.getName()))
                .sorted(comparing(f -> SegmentFile.fromPath(f.getName())))
                .collect(Collectors.toList());
    }

    private long parseStartIdFromFileName(String fileName) {
        return SegmentFile.fromPath(fileName).startId();
    }
}