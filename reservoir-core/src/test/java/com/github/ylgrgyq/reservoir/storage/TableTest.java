package com.github.ylgrgyq.reservoir.storage;

import com.github.ylgrgyq.reservoir.FileUtils;
import com.github.ylgrgyq.reservoir.SerializedObjectWithId;
import com.github.ylgrgyq.reservoir.TestingUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import static com.github.ylgrgyq.reservoir.TestingUtils.makeString;
import static com.github.ylgrgyq.reservoir.TestingUtils.numberString;
import static org.assertj.core.api.Assertions.assertThat;

public class TableTest {
    private TableBuilder builder;
    private String tempLogFileName;
    private FileChannel testingFileChannel;

    @Before
    public void setUp() throws Exception {
        String tempDir = System.getProperty("java.io.tmpdir", "/tmp") + File.separator +
                "reservoir_block_test_" + System.nanoTime();
        FileUtils.forceMkdir(new File(tempDir));
        tempLogFileName = tempDir + File.separator + "log_test";
        testingFileChannel = FileChannel.open(Paths.get(tempLogFileName), StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.READ);
        builder = new TableBuilder(testingFileChannel);
    }

    @Test
    public void testWriteReadOneData() throws Exception {
        addData(10101, "Hello");

        final long tableSize = builder.finishBuild();
        final Table table = Table.open(testingFileChannel, tableSize);
        assertThat(table.iterator())
                .toIterable()
                .containsExactly(makeObjectWithId(10101, "Hello"));
    }

    @Test
    public void testWriteReadManySmallData() throws Exception {
        final List<SerializedObjectWithId<byte[]>> expectDatas = new ArrayList<>();
        for (long i = 0; i < 10000; i++) {
            expectDatas.add(new SerializedObjectWithId<>(i, TestingUtils.numberStringBytes(i)));
            addData(i, numberString(i));
        }

        final long tableSize = builder.finishBuild();
        final Table table = Table.open(testingFileChannel, tableSize);
        assertThat(table.iterator())
                .toIterable()
                .isEqualTo(expectDatas);
    }

    @Test
    public void testWriteReadManyBigData() throws Exception {
        final List<SerializedObjectWithId<byte[]>> expectDatas = new ArrayList<>();
        for (long i = 0; i < 1000; i++) {
            expectDatas.add(makeObjectWithId(i, makeString("Hello", Constant.kMaxDataBlockSize)));
            addData(i, makeString("Hello", Constant.kMaxDataBlockSize));
        }

        final long tableSize = builder.finishBuild();
        final Table table = Table.open(testingFileChannel, tableSize);
        assertThat(table.iterator())
                .toIterable()
                .isEqualTo(expectDatas);
    }

    private SerializedObjectWithId<byte[]> makeObjectWithId(long id, String data) {
        return new SerializedObjectWithId<>(id, data.getBytes(StandardCharsets.UTF_8));
    }

    private void addData(long id, String data) throws IOException {
        builder.add(id, data.getBytes(StandardCharsets.UTF_8));
    }
}