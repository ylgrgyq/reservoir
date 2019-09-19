package com.github.ylgrgyq.reservoir.storage;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;

class BufferedChannel implements AutoCloseable {
    private final FileChannel fileChannel;
    private final ByteBuffer readBuffer;
    private final ByteBuffer writeBuffer;
    private long readBufferStartPosition;
    private long writeBufferStartPosition;

    BufferedChannel(FileChannel fileChannel) throws IOException {
        this(fileChannel, Constant.kMaxDataBlockSize, Constant.kMaxDataBlockSize);
    }

    BufferedChannel(FileChannel fileChannel, int readCapacity, int writeCapacity) throws IOException {
        Objects.requireNonNull(fileChannel, "fileChannel");

        this.fileChannel = fileChannel;
        this.readBuffer = ByteBuffer.allocate(readCapacity);
        this.readBuffer.flip();
        this.writeBuffer = ByteBuffer.allocateDirect(writeCapacity);
        this.readBufferStartPosition = fileChannel.position();
        this.writeBufferStartPosition = fileChannel.position();
    }

    synchronized void write(ByteBuffer src) throws IOException {
        while (src.hasRemaining()) {
            final ByteBuffer buf = src.slice();
            buf.limit(Math.min(writeBuffer.remaining(), buf.remaining()));
            final int pos = src.position() + buf.remaining();
            writeBuffer.put(buf);
            src.position(pos);

            // flush when buffer is full
            if (!writeBuffer.hasRemaining()) {
                flush();
            }
        }
    }

    synchronized void flush() throws IOException {
        writeBuffer.flip();
        do {
            fileChannel.write(writeBuffer);
        } while (writeBuffer.hasRemaining());

        writeBuffer.clear();
    }

    void force() throws IOException {
        fileChannel.force(true);
    }

    synchronized int read(ByteBuffer dest) throws IOException {
        return read(dest, readBufferStartPosition + readBuffer.position());
    }

    int read(ByteBuffer dest, long pos) throws IOException {
        return read(dest, pos, dest.limit());
    }

    synchronized int read(ByteBuffer dest, long pos, int length) throws IOException {
        long currentPos = pos;
        final long eof = fileChannel.size();
        if (currentPos >= eof) {
            return -1;
        }

        while (length > 0) {
            if (readBufferStartPosition <= currentPos &&
                    currentPos < readBufferStartPosition + readBuffer.limit()) {
                final int startPosInBuffer = (int) (currentPos - readBufferStartPosition);
                readBuffer.position(startPosInBuffer);
                final int bytesSize = Math.min(length, readBuffer.remaining());

                final Buffer buf = readBuffer.slice().limit(bytesSize);
                dest.put((ByteBuffer) buf);
                readBuffer.position(startPosInBuffer + bytesSize);
                currentPos += bytesSize;
                length -= bytesSize;
            } else if (currentPos >= eof) {
                break;
            } else {
                readBufferStartPosition = currentPos;
                readBuffer.clear();
                if ((fileChannel.read(readBuffer, currentPos)) <= 0) {
                    throw new IOException("EOF before read enough bytes");
                }

                readBuffer.flip();
            }
        }

        return (int) (currentPos - pos);
    }

    @Override
    public synchronized void close() throws IOException {
        fileChannel.close();
        writeBuffer.clear();
        writeBuffer.flip();
        readBuffer.clear();
        readBuffer.flip();
    }
}
