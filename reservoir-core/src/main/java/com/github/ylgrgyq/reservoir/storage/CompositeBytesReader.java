package com.github.ylgrgyq.reservoir.storage;

import java.nio.*;
import java.util.List;

class CompositeByteBufferReader {
    private final List<byte[]> buckets;
    private int index;

    public CompositeByteBufferReader(List<byte[] buckets) {
        this.buckets = buckets;
    }

    private int getBucketIndex() {
        return index / buckets.size();
    }

    private int getOffset() {
        
    }

    public byte get() {
        return 0;
    }


    public byte get(int index) {
        return 0;
    }


    public ByteBuffer compact() {
        return null;
    }


    public char getChar() {
        return 0;
    }


    public char getChar(int index) {
        return 0;
    }


    public short getShort() {
        return 0;
    }


    public short getShort(int index) {
        return 0;
    }


    public int getInt() {
        return 0;
    }


    public int getInt(int index) {
        return 0;
    }


    public long getLong() {
        return 0;
    }

    public long getLong(int index) {
        return 0;
    }


    public float getFloat() {
        return 0;
    }


    public float getFloat(int index) {
        return 0;
    }


    public double getDouble() {
        return 0;
    }


    public double getDouble(int index) {
        return 0;
    }

}
