package com.github.ylgrgyq.reservoir.storage;

import com.github.ylgrgyq.reservoir.FileUtils;
import com.github.ylgrgyq.reservoir.SerializedObjectWithId;
import com.github.ylgrgyq.reservoir.TestingUtils;
import com.github.ylgrgyq.reservoir.storage.BlockBuilder.WriteBlockResult;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import static com.github.ylgrgyq.reservoir.TestingUtils.numberString;
import static org.assertj.core.api.Assertions.assertThat;


public class BlockTest {
    private BlockBuilder builder;
    private String tempLogFileName;
    private FileChannel testingFileChannel;

    @Before
    public void setUp() throws Exception {
        builder = new BlockBuilder();

        String tempDir = System.getProperty("java.io.tmpdir", "/tmp") + File.separator +
                "reservoir_block_test_" + System.nanoTime();
        FileUtils.forceMkdir(new File(tempDir));
        tempLogFileName = tempDir + File.separator + "log_test";
        testingFileChannel = FileChannel.open(Paths.get(tempLogFileName), StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.READ);
    }

    @Test
    public void testBuilderIsEmpty() {
        assertThat(builder.isEmpty()).isTrue();
        for (int i = 0; i < 10000; ++i) {
            addData(i, TestingUtils.makeString("Hello", 128));
        }
        assertThat(builder.isEmpty()).isFalse();
        builder.reset();
        assertThat(builder.isEmpty()).isTrue();
    }

    @Test
    public void testEstimateBlockSize() {
        assertThat(builder.estimateBlockSize()).isZero();
        for (int i = 0; i < 10000; ++i) {
            addData(i, TestingUtils.makeString("Hello", 128));
        }

        long expectCheckpointNumber = 10000 / Constant.kBlockCheckpointInterval + 1;
        assertThat(builder.estimateBlockSize()).isEqualTo(builder.getBlockDataSize() + expectCheckpointNumber * Integer.BYTES + Integer.BYTES);
    }

    @Test
    public void testReset() {
        for (long i = 0; i < 10000; i++) {
            addData(i, numberString(i));
        }
        assertThat(builder.estimateBlockSize()).isGreaterThan(0);
        assertThat(builder.getBlockDataSize()).isGreaterThan(0);
        assertThat(builder.isEmpty()).isFalse();

        builder.reset();

        assertThat(builder.estimateBlockSize()).isZero();
        assertThat(builder.getBlockDataSize()).isZero();
        assertThat(builder.isEmpty()).isTrue();
    }

    @Test
    public void testWriteReadBlock() throws Exception {
        final List<SerializedObjectWithId> expectDatas = new ArrayList<>();
        for (long i = 0; i < 10000; i++) {
            expectDatas.add(new SerializedObjectWithId<>(i, TestingUtils.numberStringBytes(i)));
            addData(i, numberString(i));
        }

        final WriteBlockResult result = builder.writeBlock(testingFileChannel);
        final ByteBuffer actualBlockData = ByteBuffer.allocate(result.getWrittenBlockSize());
        testingFileChannel.read(actualBlockData, 0);

        assertThat(new Block(actualBlockData).iterator())
                .toIterable()
                .isEqualTo(expectDatas);
    }

    @Test
    public void testSeek() throws Exception {
        final List<SerializedObjectWithId<byte[]>> addedData = new ArrayList<>();
        for (long i = 0; i < 1000; i++) {
            addedData.add(new SerializedObjectWithId<>(i, TestingUtils.numberStringBytes(i)));
            addData(i, numberString(i));
        }

        final WriteBlockResult result = builder.writeBlock(testingFileChannel);
        final ByteBuffer actualBlockData = ByteBuffer.allocate(result.getWrittenBlockSize());
        testingFileChannel.read(actualBlockData, 0);
        final SeekableIterator<Long, SerializedObjectWithId<byte[]>> actualBlockIterator = new Block(actualBlockData).iterator();

        for (long i = -100; i < 2000; i++) {
            actualBlockIterator.seek(i);
            final List<SerializedObjectWithId<byte[]>> expectDatas = addedData.subList(
                    (int) Math.min(Math.max(0, i + 1), addedData.size()),
                    addedData.size());
            assertThat(actualBlockIterator)
                    .toIterable()
                    .isEqualTo(expectDatas);
        }
        assertThat(actualBlockIterator).isExhausted();
    }

    private void addData(long id, String data) {
        builder.add(id, data.getBytes(StandardCharsets.UTF_8));
    }
}