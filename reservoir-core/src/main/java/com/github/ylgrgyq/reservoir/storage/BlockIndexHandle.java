package com.github.ylgrgyq.reservoir.storage;

import java.nio.ByteBuffer;
import java.util.Objects;

final class BlockIndexHandle {
    static int BLOCK_HANDLE_SIZE = Long.BYTES + Integer.BYTES;

    private final long blockStartOffset;
    private final int blockSize;

    BlockIndexHandle(final long blockStartOffset, final int blockSize) {
        this.blockStartOffset = blockStartOffset;
        this.blockSize = blockSize;
    }

    long getBlockStartOffset() {
        return blockStartOffset;
    }

    int getBlockSize() {
        return blockSize;
    }

    byte[] encode() {
        ByteBuffer buffer = ByteBuffer.allocate(BLOCK_HANDLE_SIZE);
        buffer.putLong(blockStartOffset);
        buffer.putInt(blockSize);

        return buffer.array();
    }

    static BlockIndexHandle decode(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long offset =  buffer.getLong();
        int size = buffer.getInt();
        return new BlockIndexHandle(offset, size);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final BlockIndexHandle handle = (BlockIndexHandle) o;
        return getBlockStartOffset() == handle.getBlockStartOffset() &&
                getBlockSize() == handle.getBlockSize();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getBlockStartOffset(), getBlockSize());
    }

    @Override
    public String toString() {
        return "BlockIndexHandle{" +
                "blockStartOffset=" + blockStartOffset +
                ", blockSize=" + blockSize +
                '}';
    }
}

