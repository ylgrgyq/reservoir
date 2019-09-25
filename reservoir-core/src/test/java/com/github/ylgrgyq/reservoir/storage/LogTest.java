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
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Random;
import java.util.zip.CRC32;

import static com.github.ylgrgyq.reservoir.TestingUtils.makeString;
import static com.github.ylgrgyq.reservoir.TestingUtils.nextPositiveInt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class LogTest {
    private String tempLogFile;
    private LogWriter logWriter;
    private LogReader logReader;
    private FileChannel readChannel;

    @Before
    public void setUp() throws Exception {
        String tempDir = System.getProperty("java.io.tmpdir", "/tmp") + File.separator +
                "reservoir_log_test_" + System.nanoTime();
        FileUtils.forceMkdir(new File(tempDir));
        this.tempLogFile = tempDir + File.separator + "log_test";

        logWriter = new LogWriter(Paths.get(tempLogFile), StandardOpenOption.CREATE,
                StandardOpenOption.WRITE);
        readChannel = FileChannel.open(Paths.get(tempLogFile),
                StandardOpenOption.READ);
        logReader = new LogReader(readChannel, true);
    }

    @After
    public void tearDown() throws Exception {
        logWriter.close();
        logReader.close();
    }

    @Test
    public void simpleReadWriteLog() throws Exception {
        writeLog("Hello");
        writeLog("World");
        writeLog("!");

        assertThat(readLog()).isEqualTo("Hello");
        assertThat(readLog()).isEqualTo("World");
        assertThat(readLog()).isEqualTo("!");
        assertThat(readLog()).isEqualTo("EOF");
        assertThat(readLog()).isEqualTo("EOF");
    }

    @Test
    public void simpleReadWriteLotsLog() throws Exception {
        for (int i = 0; i < 10000; i++) {
            writeLog(TestingUtils.numberString(i));
        }

        for (int i = 0; i < 10000; i++) {
            assertThat(readLog()).isEqualTo(TestingUtils.numberString(i));
        }

        assertThat(readLog()).isEqualTo("EOF");
    }

    @Test
    public void simpleReadWriteRandomLengthLog() throws Exception {
        Random random = new Random(101);

        // we must have a bound to ensure test will not OOM
        for (int i = 0; i < 100; i++) {
            writeLog(makeString("Hello", nextPositiveInt(random, 2 * Constant.kLogBlockSize)));
        }

        random = new Random(101);
        for (int i = 0; i < 100; i++) {
            assertThat(readLog()).isEqualTo(makeString("Hello", nextPositiveInt(random, 2 * Constant.kLogBlockSize)));
        }

        assertThat(readLog()).isEqualTo("EOF");
    }

    @Test
    public void simpleReadWriteFragmentedLog() throws Exception {
        writeLog("Small");
        writeLog(makeString("HalfBlock", Constant.kLogBlockSize / 2));
        writeLog(makeString("WholeBlock", Constant.kLogBlockSize));
        writeLog(makeString("MegaBlock", 100 * Constant.kLogBlockSize));

        assertThat(readLog()).isEqualTo("Small");
        assertThat(readLog()).isEqualTo(makeString("HalfBlock", Constant.kLogBlockSize / 2));
        assertThat(readLog()).isEqualTo(makeString("WholeBlock", Constant.kLogBlockSize));
        assertThat(readLog()).isEqualTo(makeString("MegaBlock", 100 * Constant.kLogBlockSize));

        assertThat(readLog()).isEqualTo("EOF");
    }

    @Test
    public void readWriteTrailer() throws Exception {
        // write a log which only leave a header size space in a block
        writeLog(makeString("Hello", Constant.kLogBlockSize - 2 * Constant.kLogHeaderSize));
        assertThat(logWriter.fileChannel().size()).isEqualTo(Constant.kLogBlockSize - Constant.kLogHeaderSize);
        // write a new log which will be write to a new block
        writeLog("World");

        assertThat(readLog()).isEqualTo(makeString("Hello", Constant.kLogBlockSize - 2 * Constant.kLogHeaderSize));
        assertThat(readLog()).isEqualTo("World");

        assertThat(readLog()).isEqualTo("EOF");
    }

    @Test
    public void paddingBlock() throws Exception {
        // write a log which leaves space in block shorter than the header size
        writeLog(makeString("Hello", Constant.kLogBlockSize - 2 * Constant.kLogHeaderSize + 6));
        assertThat(logWriter.fileChannel().size()).isEqualTo(Constant.kLogBlockSize - Constant.kLogHeaderSize + 6);
        // write a new log which will be write to a new block
        writeLog("World");

        assertThat(readLog()).isEqualTo(makeString("Hello", Constant.kLogBlockSize - 2 * Constant.kLogHeaderSize + 6));
        assertThat(readLog()).isEqualTo("World");

        assertThat(readLog()).isEqualTo("EOF");
    }

    @Test
    public void testReopenWriter() throws Exception {
        writeLog("Hello");
        reopenWriter();
        writeLog("World");
        writeLog("!");

        assertThat(readLog()).isEqualTo("Hello");
        assertThat(readLog()).isEqualTo("World");
        assertThat(readLog()).isEqualTo("!");
        assertThat(readLog()).isEqualTo("EOF");
        assertThat(readLog()).isEqualTo("EOF");
    }

    @Test
    public void readUnfinishedHeader() throws Exception {
        writeLog("Hello");

        truncateLogFile(8);
        assertThat(readLog()).isEqualTo("EOF");
    }

    @Test
    public void readLogFileFailed() throws Exception {
        writeLog("Hello");

        readChannel.close();

        assertThatThrownBy(this::readLog).isInstanceOf(IOException.class);
    }

    @Test
    public void unknownRecordType() throws Exception {
        writeLog("Hello");
        // checksum size + log length = 10 bytes
        // 25 is an arbitrary value which is not a valid record type code
        assert Constant.kLogHeaderSize == 11;
        setByteInFile(10, (byte) 25);
        assertThatThrownBy(this::readLog)
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("unknown record type code: 25");
    }

    @Test
    public void badLength() throws Exception {
        writeLog(makeString("Hello", Constant.kLogBlockSize / 2));
        long pos = logWriter.fileChannel().position();
        writeLog(makeString("World", Constant.kLogBlockSize));
        assert Constant.kLogHeaderSize == 11;
        // set length of the second log to 32767
        setByteInFile(pos + 8, (byte) 127);
        setByteInFile(pos + 9, (byte) 255);
        assertThat(readLog()).isEqualTo(makeString("Hello", Constant.kLogBlockSize / 2));
        assertThatThrownBy(this::readLog)
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("block buffer under flow.");
    }

    @Test
    public void checksumFailed() throws Exception {
        writeLog("Hello");
        setByteInFile(2, (byte) 127);
        assertThatThrownBy(this::readLog)
                .isInstanceOf(StorageException.class)
                .hasMessageMatching("checksum: \\d+.*expect:.*");
    }

    @Test
    public void readHeaderUnfinishedLog() throws Exception {
        writeLog("Hello");
        long pos = logWriter.fileChannel().position();
        writeLog("World");
        assert Constant.kLogHeaderSize == 11;
        truncateLogFile(pos + 5);
        assertThat(readLog()).isEqualTo("Hello");
        assertThat(readLog()).isEqualTo("EOF");
    }

    @Test
    public void readDataUnfinishedLog() throws Exception {
        writeLog("Hello");
        long pos = logWriter.fileChannel().position();
        writeLog("World");
        assert Constant.kLogHeaderSize == 11;
        truncateLogFile(pos + 12);
        assertThat(readLog()).isEqualTo("Hello");
        assertThat(readLog()).isEqualTo("EOF");
    }

    @Test
    public void missingStartForMiddleRecord() throws Exception {
        writeLog("Hello");
        setByteInFile(10, RecordType.kMiddleType.getCode());
        fixChecksum(0);
        assertThatThrownBy(this::readLog)
                .isInstanceOf(StorageException.class)
                .hasMessage("missing start of fragmented record");
    }

    @Test
    public void missingStartForLastRecord() throws Exception {
        writeLog("Hello");
        setByteInFile(10, RecordType.kLastType.getCode());
        fixChecksum(0);
        assertThatThrownBy(this::readLog)
                .isInstanceOf(StorageException.class)
                .hasMessage("missing start for the last fragmented record");
    }

    @Test
    public void unexpectedFullType() throws Exception {
        writeLog("Hello");
        writeLog("World");
        setByteInFile(10, RecordType.kFirstType.getCode());
        fixChecksum(0);
        assertThatThrownBy(this::readLog)
                .isInstanceOf(StorageException.class)
                .hasMessage("partial record without end(1)");
    }

    @Test
    public void unexpectedFirstType() throws Exception {
        writeLog("Hello");
        writeLog(makeString("World", Constant.kLogBlockSize));
        setByteInFile(10, RecordType.kFirstType.getCode());
        fixChecksum(0);
        assertThatThrownBy(this::readLog)
                .isInstanceOf(StorageException.class)
                .hasMessage("partial record without end(2)");
    }

    @Test
    public void missingLastRecordHeaderIsIgnored() throws Exception {
        writeLog(makeString("Hello", Constant.kLogBlockSize));

        // there's 2 * kLogHeaderSize bytes left in second block including header
        // truncate all of them
        truncateLogFile(logWriter.fileChannel().size() - 2 * Constant.kLogHeaderSize);
        assertThat(readLog()).isEqualTo("EOF");
    }

    @Test
    public void missingLastRecordDataIsIgnored() throws Exception {
        writeLog(makeString("Hello", Constant.kLogBlockSize));

        // there's 2 * kLogHeaderSize bytes left in second block including header
        // truncate some data and leaves header and some of the data block
        truncateLogFile(logWriter.fileChannel().size() - Constant.kLogHeaderSize / 2);
        assertThat(readLog()).isEqualTo("EOF");
    }

    private void writeLog(String log) throws IOException {
        logWriter.append(log.getBytes(StandardCharsets.UTF_8));
        logWriter.flush(false);
    }

    private String readLog() throws IOException, StorageException {
        List<byte[]> logs = logReader.readLog();
        if (logs.isEmpty()) {
            return "EOF";
        } else {
            return new String(concatByteArray(logs), StandardCharsets.UTF_8);
        }
    }

    private byte[] concatByteArray(List<byte[]> out) {
        final byte[] ret = new byte[out.stream().mapToInt(bs -> bs.length).sum()];
        int len = 0;
        for (byte[] bs : out) {
            System.arraycopy(bs, 0, ret, len, bs.length);
            len += bs.length;
        }
        return ret;
    }

    private void reopenWriter() throws Exception {
        logWriter.close();
        logWriter = new LogWriter(Paths.get(tempLogFile), StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    }

    private void setByteInFile(long position, byte newValue) throws Exception {
        final ByteBuffer valBuf = ByteBuffer.allocate(1);
        valBuf.put(newValue);
        valBuf.flip();

        try (FileChannel channel = FileChannel.open(Paths.get(tempLogFile), StandardOpenOption.WRITE)) {
            channel.write(valBuf, position);
        }
    }

    private void truncateLogFile(long expectSize) throws Exception {
        try (FileChannel channel = FileChannel.open(Paths.get(tempLogFile), StandardOpenOption.WRITE)) {
            channel.truncate(expectSize);
        }
    }

    private void fixChecksum(long headerOffset) throws Exception {
        try (FileChannel channel = FileChannel.open(Paths.get(tempLogFile),
                StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            channel.position(headerOffset);

            ByteBuffer header = ByteBuffer.allocate(Constant.kLogHeaderSize);
            channel.read(header);
            header.flip();

            header.getLong();
            short len = header.getShort();
            byte typeCode = header.get();

            ByteBuffer data = ByteBuffer.allocate(len);
            channel.position(headerOffset + Constant.kLogHeaderSize);
            channel.read(data);
            data.flip();

            final CRC32 actualChecksum = new CRC32();
            actualChecksum.update(typeCode);
            actualChecksum.update(data.array());

            ByteBuffer newChecksum = ByteBuffer.allocate(Long.BYTES);
            newChecksum.putLong(actualChecksum.getValue());
            newChecksum.flip();
            channel.write(newChecksum, headerOffset);
        }
    }
}