package com.github.ylgrgyq.reservoir.storage;

import com.github.ylgrgyq.reservoir.FileUtils;
import com.github.ylgrgyq.reservoir.StorageException;
import com.github.ylgrgyq.reservoir.TestingUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class BufferedChannelTest {
    private String tempLogFile;
    private FileChannel fileChannel;
    private BufferedChannel channel;

    @Before
    public void setUp() throws Exception {
        String tempDir = System.getProperty("java.io.tmpdir", "/tmp") + File.separator +
                "reservoir_log_test_" + System.nanoTime();
        FileUtils.forceMkdir(new File(tempDir));
        this.tempLogFile = tempDir + File.separator + "log_test";

        fileChannel = FileChannel.open(Paths.get(tempLogFile), StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.READ);
        channel = new BufferedChannel(fileChannel);
    }

    @After
    public void tearDown() throws Exception {
        channel.close();
    }

    @Test
    public void testSimpleWriteRead() throws Exception {
        write("Hello");
        write("World");
        write("!");

        assertThat(readString(5)).isEqualTo("Hello");
        assertThat(readString(5)).isEqualTo("World");
        assertThat(readString(1)).isEqualTo("!");
        assertThat(readString(1)).isEqualTo("EOF");
        assertThat(readString(1)).isEqualTo("EOF");
    }

    @Test
    public void testWriteReadManyData() throws Exception {
        for (int i = 0; i < 10000; i++) {
            write(TestingUtils.numberString(i));
        }

        for (int i = 0; i < 10000; i++) {
            assertThat(readString(TestingUtils.numberString(i).getBytes(StandardCharsets.UTF_8).length))
                    .isEqualTo(TestingUtils.numberString(i));
        }

        assertThat(readString(1)).isEqualTo("EOF");
    }

    @Test
    public void testWriteBiggerThanWriteBuffer() throws Exception {
        String expect = TestingUtils.makeString("Hello", 2 * Constant.kMaxDataBlockSize);
        write(expect);

        assertThat(readString((int)fileChannel.size())).isEqualTo(expect);
    }

    @Test
    public void testReadAfterClose() throws Exception {
        write("Hello");
        channel.close();
        assertThatThrownBy(() -> readString(100)).isInstanceOf(ClosedChannelException.class);
    }

    @Test
    public void testWriteAfterClose() throws Exception {
        write("Hello");
        channel.close();
        assertThatThrownBy(() -> write("Hello")).isInstanceOf(ClosedChannelException.class);
    }

    private void write(String log) throws IOException {
        channel.write(ByteBuffer.wrap(log.getBytes(StandardCharsets.UTF_8)));
        channel.flush();
    }

    private String readString(int bytesToRead) throws IOException, StorageException {
        ByteBuffer buf = ByteBuffer.allocate(bytesToRead);
        if (channel.read(buf) == -1) {
            return "EOF";
        } else {
            return new String(buf.array(), StandardCharsets.UTF_8);
        }
    }
}