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
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.github.ylgrgyq.reservoir.storage2.StorageTestingUtils.generateTestingBytes;
import static org.assertj.core.api.Assertions.assertThat;

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

    @Test
    public void testReadAndWriteWithMultiSegments() throws IOException {
        final List<ByteBuffer> data = prepareDataInLog(10, 100);
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
    public void testRecovery() throws Exception {
        final List<ByteBuffer> data = prepareDataInLog(10, 100);
        final Log recoveredLog = new Log(testingDirPath, logName, maxSegmentSize);
        for (long i = 0; i < data.size(); i++) {
            final FileRecords records = recoveredLog.read(i);
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
        assertThat(recoveredLog.size()).isEqualTo(testingLog.size());
    }

    @Test
    public void testRecoverEmptySegment() throws Exception {
        final List<ByteBuffer> data = prepareDataInLog(10, 100);
        final File[] files = testingDirPath.toFile().listFiles();
        assert files != null;
//        final Optional<Path> segmentPaths = Arrays.stream(files)
//                .filter(File::isFile)
//                .filter(f -> FileType.parseFileType(f.getName()) == FileType.Segment)
//                .sorted((o1, o2) -> {
//                    o1.
//                    final BasicFileAttributes attr = Files.readAttributes(o1.toPath(), BasicFileAttributes.class);
//                    createT = attr.creationTime().toInstant();
//                    return -1;
//                }).map(File::toPath).findFirst();


        final Log recoveredLog = new Log(testingDirPath, logName, maxSegmentSize);
    }

    @Test
    public void testRecoverNonConsecutiveSegments() {

    }

    @Test
    public void testPurgeSegments() {

    }

    @Test
    public void testSkipPurgeWhenOnlyOneSegmentRemain() {

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
}