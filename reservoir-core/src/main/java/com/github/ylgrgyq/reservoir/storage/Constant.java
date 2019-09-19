package com.github.ylgrgyq.reservoir.storage;

final class Constant {
    // Header is checksum (Long) + length (Short) + record type (Byte).
    static final int kLogHeaderSize = Long.BYTES + Short.BYTES + Byte.BYTES;
    static final int kLogBlockSize = 32768;

    static final int kMaxDataBlockSize = 4096;

    // Block Trailer is checksum(Long)
    static final int kBlockTrailerSize = Long.BYTES;

    static final long kTableMagicNumber = 24068102;

    static final int kMaxMemtableSize = 8388608;

    // Add a checkpoint every this many key values in a block
    static final int kBlockCheckpointInterval = 64;
}
