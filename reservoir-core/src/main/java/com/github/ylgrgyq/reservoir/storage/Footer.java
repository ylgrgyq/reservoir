package com.github.ylgrgyq.reservoir.storage;

import java.nio.ByteBuffer;
import java.util.Objects;

final class Footer {
    static int tableFooterSize = BlockIndexHandle.BLOCK_HANDLE_SIZE + Long.BYTES;

    static Footer decode(byte[] bytes) {
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        final byte[] indexBlockHandleBytes = new byte[BlockIndexHandle.BLOCK_HANDLE_SIZE];
        buffer.get(indexBlockHandleBytes);

        final long magic = buffer.getLong();
        if (magic != Constant.kTableMagicNumber) {
            throw new IllegalStateException("found invalid sstable during checking magic number");
        }

        final BlockIndexHandle blockIndexHandle = BlockIndexHandle.decode(indexBlockHandleBytes);
        return new Footer(blockIndexHandle);
    }

    private final BlockIndexHandle blockIndexHandle;

    Footer(BlockIndexHandle blockIndexHandle) {
        this.blockIndexHandle = blockIndexHandle;
    }

    BlockIndexHandle getBlockIndexHandle() {
        return blockIndexHandle;
    }

    byte[] encode() {
        final byte[] indexBlock = blockIndexHandle.encode();
        assert indexBlock.length == BlockIndexHandle.BLOCK_HANDLE_SIZE;
        final ByteBuffer buffer = ByteBuffer.allocate(BlockIndexHandle.BLOCK_HANDLE_SIZE + Long.BYTES);
        buffer.put(indexBlock);
        buffer.putLong(Constant.kTableMagicNumber);

        return buffer.array();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Footer footer = (Footer) o;
        return getBlockIndexHandle().equals(footer.getBlockIndexHandle());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getBlockIndexHandle());
    }

    @Override
    public String toString() {
        return "Footer{" +
                "blockIndexHandle=" + blockIndexHandle +
                '}';
    }
}
