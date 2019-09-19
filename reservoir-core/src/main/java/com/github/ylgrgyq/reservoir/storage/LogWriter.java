package com.github.ylgrgyq.reservoir.storage;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;

final class LogWriter implements Closeable {
    private final BufferedChannel workingFileChannel;
    private final ByteBuffer headerBuffer;
    private final ByteBuffer zeros;
    private final CRC32 checksum;
    private int blockOffset;

    LogWriter(FileChannel workingFileChannel) throws IOException {
        this(workingFileChannel, 0);
    }

    LogWriter(FileChannel workingFileChannel, long writePosition) throws IOException {
        workingFileChannel.position(writePosition);
        this.workingFileChannel = new BufferedChannel(workingFileChannel);
        this.headerBuffer = ByteBuffer.allocateDirect(Constant.kLogHeaderSize);
        this.blockOffset = 0;
        this.zeros = ByteBuffer.allocate(Constant.kLogHeaderSize);
        this.checksum = new CRC32();
    }

    void flush(boolean force) throws IOException {
        workingFileChannel.flush();
        if (force) {
            workingFileChannel.force();
        }
    }

    @Override
    public void close() throws IOException {
        flush(true);
        workingFileChannel.close();
    }

    void append(byte[] data) throws IOException {
        assert data.length > 0;

        final ByteBuffer writeBuffer = ByteBuffer.wrap(data);
        int dataSizeRemain = writeBuffer.remaining();
        boolean begin = true;

        while (dataSizeRemain > 0) {
            final int blockLeft = Constant.kLogBlockSize - blockOffset;
            assert blockLeft >= 0;

            // we don't expect data.length == 0, so if blockLeft == kLogHeaderSize
            // we need to allocate another block also
            if (blockLeft <= Constant.kLogHeaderSize) {
                paddingBlock(blockLeft);
                blockOffset = 0;
            }

            // Invariant: never leave < kLogHeaderSize bytes in a block
            assert Constant.kLogBlockSize - blockOffset - Constant.kLogHeaderSize >= 0;

            final RecordType type;
            final int blockForDataAvailable = Constant.kLogBlockSize - blockOffset - Constant.kLogHeaderSize;
            final int fragmentSize = Math.min(blockForDataAvailable, dataSizeRemain);
            final boolean end = fragmentSize == dataSizeRemain;
            if (begin && end) {
                type = RecordType.kFullType;
            } else if (begin) {
                type = RecordType.kFirstType;
            } else if (end) {
                type = RecordType.kLastType;
            } else {
                type = RecordType.kMiddleType;
            }

            byte[] out = new byte[fragmentSize];
            writeBuffer.get(out);
            writeRecord(type, out);

            begin = false;
            dataSizeRemain -= fragmentSize;
        }
    }

    private void paddingBlock(int blockLeft) throws IOException {
        assert blockLeft >= 0 : "blockLeft: " + blockLeft;

        if (blockLeft > 0) {
            // padding with bytes array full of zero
            final ByteBuffer padding = zeros;
            padding.position(Constant.kLogHeaderSize - blockLeft);
            workingFileChannel.write(padding);
        }
    }

    private void writeRecord(RecordType type, byte[] blockPayload) throws IOException {
        assert blockOffset + Constant.kLogHeaderSize + blockPayload.length <= Constant.kLogBlockSize;

        // checksum includes the record type and record payload
        final CRC32 crc32 = checksum;
        crc32.reset();
        crc32.update(type.getCode());
        crc32.update(blockPayload);

        // format header
        final ByteBuffer header = headerBuffer;
        header.clear();
        header.putLong(crc32.getValue());
        header.putShort((short) blockPayload.length);
        header.put(type.getCode());
        header.flip();

        // write header and payload
        final BufferedChannel ch = workingFileChannel;
        ch.write(header);
        ch.write(ByteBuffer.wrap(blockPayload));
        blockOffset += blockPayload.length + Constant.kLogHeaderSize;
    }
}
