package com.github.ylgrgyq.reservoir.storage;

import java.nio.*;
import java.util.List;

class CompositeBytesReader {
    private final List<byte[]> buckets;
    private int bucketIndex;
    private int offset;

    public CompositeBytesReader(List<byte[]> buckets) {
        this.buckets = buckets;
    }

    private int getBucketIndex() {
        return bucketIndex / buckets.size();
    }

    public byte get() {
        byte[] bucket;
        for (bucket = buckets.get(bucketIndex); offset >= bucket.length; ){
            offset = 0;
            ++bucketIndex;
        }

        return bucket[offset++];
    }

//    public char getChar() {
//        byte[] bucket;
//        for (bucket = buckets.get(bucketIndex); offset >= bucket.length; ){
//            offset = 0;
//            ++bucketIndex;
//        }
//        return bucket[offset++];
//    }

    public short getShort() {
        return 0;
    }

    public int getInt() {
        return 0;
    }

    public long getLong() {
        return 0;
    }

    public float getFloat() {
        return 0;
    }

    public double getDouble() {
        return 0;
    }
}
