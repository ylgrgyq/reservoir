package com.github.ylgrgyq.reservoir.storage;

import org.junit.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class BitsTest {
    @Test
    public void testReadWriteByte() {
        final byte[] bs = new byte[100];
        for (int offset = 0; offset < bs.length; offset++) {
            Bits.putByte(bs, offset, (byte) offset);
            assertThat(Bits.getByte(bs, offset)).isEqualTo((byte) offset);
        }
    }

    @Test
    public void testReadWriteBytes() {
        final byte[] bs = new byte[100];
        final byte[] data = new byte[6];
        Arrays.fill(data, (byte)0xAB);
        for (int offset = 0; offset < bs.length - data.length; offset++) {
            Bits.putBytes(bs, offset, data);
            assertThat(Bits.getBytes(bs, offset, data.length)).isEqualTo(data);
        }
    }

    @Test
    public void testReadWriteShort() {
        final byte[] bs = new byte[100];

        for (int offset = 0; offset < bs.length - Short.BYTES; offset += Short.BYTES) {
            Bits.putShort(bs, offset, (short) offset);
            assertThat(Bits.getShort(bs, offset)).isEqualTo((short) offset);
        }
    }

    @Test
    public void testReadWriteInt() {
        final byte[] bs = new byte[100];

        for (int offset = 0; offset < bs.length - Integer.BYTES; offset += Integer.BYTES) {
            Bits.putInt(bs, offset, offset);
            assertThat(Bits.getInt(bs, offset)).isEqualTo(offset);
        }
    }

    @Test
    public void testReadWriteLong() {
        final byte[] bs = new byte[100];

        for (int offset = 0; offset < bs.length - Long.BYTES; offset += Long.BYTES) {
            Bits.putLong(bs, offset, (long) offset);
            assertThat(Bits.getLong(bs, offset)).isEqualTo((long) offset);
        }
    }

    @Test
    public void testReadWriteFloat() {
        final byte[] bs = new byte[100];

        for (int offset = 0; offset < bs.length - Float.BYTES; offset += Float.BYTES) {
            Bits.putFloat(bs, offset, Float.intBitsToFloat(offset));
            assertThat(Bits.getFloat(bs, offset)).isEqualTo(Float.intBitsToFloat(offset));
        }
    }

    @Test
    public void testReadWriteDouble() {
        final byte[] bs = new byte[100];

        for (int offset = 0; offset < bs.length - Double.BYTES; offset += Double.BYTES) {
            Bits.putDouble(bs, offset, Double.longBitsToDouble((long)offset));
            assertThat(Bits.getDouble(bs, offset)).isEqualTo(Double.longBitsToDouble((long)offset));
        }
    }
}