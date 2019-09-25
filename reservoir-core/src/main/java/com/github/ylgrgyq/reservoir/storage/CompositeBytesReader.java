package com.github.ylgrgyq.reservoir.storage;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

class CompositeBytesReader {
    static CompositeBytesReader emptyReader = new CompositeBytesReader(Collections.emptyList());

    private final List<byte[]> buckets;
    private int bucketIndex;
    private int offset;
    @Nullable
    private byte[] workingBucket;

    CompositeBytesReader(List<byte[]> buckets) {
        this.buckets = buckets;
        this.workingBucket = buckets.isEmpty() ? null : buckets.get(0);
    }

    boolean isEmpty() {
        if (buckets.isEmpty()) {
            return true;
        }

        return buckets.stream().noneMatch(bytes -> bytes.length != 0);
    }

    byte get() {
        final byte[] bucket = forwardToNextNonEmptyBucket();
        return bucket[offset++];
    }

    short getShort() {
        return (short) (((get() & 0xFF) << 8) +
                (get() & 0xFF));
    }

    int getInt() {
        return (get() << 24) +
                ((get() & 0xFF) << 16) +
                ((get() & 0xFF) << 8) +
                (get() & 0xFF);
    }

    long getLong() {
        return ((long) get() << 56) +
                ((get() & 0xFFL) << 48) +
                ((get() & 0xFFL) << 40) +
                ((get() & 0xFFL) << 32) +
                ((get() & 0xFFL) << 24) +
                ((get() & 0xFFL) << 16) +
                ((get() & 0xFFL) << 8) +
                (get() & 0xFFL);
    }

    byte[] getBytes(int length) {
        final byte[] bs = new byte[length];
        int len;
        for (int offsetInBs = 0; offsetInBs < length; offsetInBs += len) {
            forwardToNextNonEmptyBucket();
            assert workingBucket != null;
            len = Math.min(length, workingBucket.length - offset);
            System.arraycopy(workingBucket, offset, bs, offsetInBs, len);
            offset += len;
        }
        return bs;
    }

    byte[] compact() {
        final int size = buckets.stream().mapToInt(b -> b.length).sum();
        final ByteBuffer buffer = ByteBuffer.allocate(size);
        for (byte[] bytes : buckets) {
            buffer.put(bytes);
        }
        return buffer.array();
    }

    private byte[] forwardToNextNonEmptyBucket() {
        assert workingBucket != null;
        byte[] bucket;
        for (bucket = workingBucket; offset >= bucket.length; bucket = buckets.get(++bucketIndex)) {
            offset = 0;
        }
        workingBucket = bucket;
        return bucket;
    }
}
