package com.github.ylgrgyq.reservoir.storage;

import java.util.List;

class CompositeBytesReader {
    private final List<byte[]> buckets;
    private int bucketIndex;
    private int offset;
    private byte[] workingBucket;

    CompositeBytesReader(List<byte[]> buckets) {
        assert !buckets.isEmpty();

        this.buckets = buckets;
        this.workingBucket = buckets.get(0);
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
        for (int offsetInBs = 0; offsetInBs < length; ) {
            forwardToNextNonEmptyBucket();
            int len = Math.min(length, workingBucket.length - offset);
            System.arraycopy(workingBucket, offset, bs, offsetInBs, len);
            offsetInBs += len;
            offset += len;
        }
        return bs;
    }

    private byte[] forwardToNextNonEmptyBucket() {
        byte[] bucket;
        for (bucket = workingBucket; offset >= bucket.length; bucket = buckets.get(++bucketIndex)) {
            offset = 0;
        }
        workingBucket = bucket;
        return bucket;
    }
}
