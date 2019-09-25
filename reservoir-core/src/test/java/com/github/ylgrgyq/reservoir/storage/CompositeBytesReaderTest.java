package com.github.ylgrgyq.reservoir.storage;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class CompositeBytesReaderTest {
    @Test
    public void testEmptyBuckets() {
        final CompositeBytesReader reader = new CompositeBytesReader(Collections.emptyList());
        assertThat(reader.isEmpty()).isTrue();
    }

    @Test
    public void testEveryBucketIsEmpty() {
        List<byte[]> buckets = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            buckets.add(new byte[0]);
        }

        final CompositeBytesReader reader = new CompositeBytesReader(buckets);
        assertThat(reader.isEmpty()).isTrue();
    }

    @Test
    public void testGetIntFromOneByteArray() {
        final byte[] bucket = new byte[100];
        fillBytes(bucket, Integer.MAX_VALUE);
        final CompositeBytesReader reader = new CompositeBytesReader(Collections.singletonList(bucket));
        assertThat(reader.getInt()).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    public void testGetIntFromTwoByteArray() {
        final byte[] intInBytes = new byte[100];
        fillBytes(intInBytes, Integer.MIN_VALUE);

        for (int splitIndex = 0; splitIndex < intInBytes.length; splitIndex++) {
            List<byte[]> buckets = splitBytesToTwoByteArray(intInBytes, splitIndex);

            final CompositeBytesReader reader = new CompositeBytesReader(buckets);
            assertThat(reader.getInt()).isEqualTo(Integer.MIN_VALUE);
        }
    }

    @Test
    public void testGetIntFromMultiBuckets() {
        final byte[] intInBytes = new byte[100];
        fillBytes(intInBytes, Integer.MAX_VALUE);

        final List<byte[]> buckets = splitEachByte(intInBytes);

        final CompositeBytesReader reader = new CompositeBytesReader(buckets);
        assertThat(reader.getInt()).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    public void testGetLongFromOneByteArray() {
        final byte[] longInBytes = new byte[100];
        fillBytes(longInBytes, Long.MAX_VALUE);
        final CompositeBytesReader reader = new CompositeBytesReader(Collections.singletonList(longInBytes));
        assertThat(reader.getLong()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    public void testGetLongFromTwoByteArray() {
        final byte[] longInBytes = new byte[100];
        fillBytes(longInBytes, Long.MAX_VALUE);

        for (int splitIndex = 0; splitIndex < longInBytes.length; splitIndex++) {
            List<byte[]> buckets = splitBytesToTwoByteArray(longInBytes, splitIndex);

            final CompositeBytesReader reader = new CompositeBytesReader(buckets);
            assertThat(reader.getLong()).isEqualTo(Long.MAX_VALUE);
        }
    }

    @Test
    public void testGetLongFromMultiBuckets() {
        final byte[] longInBytes = new byte[100];
        fillBytes(longInBytes, Long.MAX_VALUE);

        final List<byte[]> buckets = splitEachByte(longInBytes);

        final CompositeBytesReader reader = new CompositeBytesReader(buckets);
        assertThat(reader.getLong()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    public void testGetByteArrayFromOneBucket() {
        final byte[] longInBytes = new byte[100];
        fillBytes(longInBytes, Integer.MAX_VALUE);

        final CompositeBytesReader reader = new CompositeBytesReader(Collections.singletonList(longInBytes));
        assertThat(reader.getBytes(50)).isEqualTo(Arrays.copyOfRange(longInBytes, 0, 50));
    }

    @Test
    public void testGetByteArrayFromTwoBucket() {
        final byte[] longInBytes = new byte[100];
        fillBytes(longInBytes, Long.MAX_VALUE);

        for (int splitIndex = 0; splitIndex < longInBytes.length; splitIndex++) {
            List<byte[]> buckets = splitBytesToTwoByteArray(longInBytes, splitIndex);

            final CompositeBytesReader reader = new CompositeBytesReader(buckets);
            assertThat(reader.getBytes(100)).isEqualTo(longInBytes);
        }
    }

    @Test
    public void testGetByteArrayFromMultiBuckets() {
        final byte[] longInBytes = new byte[100];
        fillBytes(longInBytes, Long.MAX_VALUE);

        final List<byte[]> buckets = splitEachByte(longInBytes);

        final CompositeBytesReader reader = new CompositeBytesReader(buckets);
        assertThat(reader.getLong()).isEqualTo(Long.MAX_VALUE);
    }

    private void fillBytes(byte[] bytes, int num) {
        int off;
        for (off = 0; off < bytes.length - 4; off += 4) {
            Bits.putInt(bytes, off, num);
        }

        final int paddingLen = bytes.length - off;
        final byte[] padding = new byte[4];
        Bits.putInt(padding, 0, num);
        System.arraycopy(padding, 0, bytes, off, paddingLen);
    }

    private void fillBytes(byte[] bytes, long num) {
        int off;
        for (off = 0; off < bytes.length - 8; off += 8) {
            Bits.putLong(bytes, off, num);
        }

        final int paddingLen = bytes.length - off;
        final byte[] padding = new byte[8];
        Bits.putLong(padding, 0, num);
        System.arraycopy(padding, 0, bytes, off, paddingLen);
    }

    private List<byte[]> splitBytesToTwoByteArray(byte[] bytes, int splitIndex){
        List<byte[]> buckets = new ArrayList<>();
        byte[] b1 = new byte[splitIndex];
        System.arraycopy(bytes, 0, b1, 0, splitIndex);
        buckets.add(b1);
        byte[] b2 = new byte[bytes.length - splitIndex];
        System.arraycopy(bytes, splitIndex, b2, 0, bytes.length - splitIndex);
        buckets.add(b2);
        return buckets;
    }

    private List<byte[]> splitEachByte(byte[] bytes) {
        final List<byte[]> buckets = new ArrayList<>();
        for (byte b : bytes) {
            buckets.add(new byte[]{b});
            buckets.add(new byte[0]);
        }
        return buckets;
    }
}