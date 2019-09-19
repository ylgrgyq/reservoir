package com.github.ylgrgyq.reservoir.storage;

import com.github.ylgrgyq.reservoir.SerializedObjectWithId;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

final class Block implements Iterable<SerializedObjectWithId<byte[]>> {
    private final ByteBuffer content;
    private final List<Integer> checkpoints;

    Block(ByteBuffer content) {
        this.content = content;
        content.position(content.limit() - Integer.BYTES);
        final int checkpointSize = content.getInt();
        assert checkpointSize > 0;

        this.checkpoints = new ArrayList<>(checkpointSize);
        final int checkpointStart = content.limit() - Integer.BYTES - checkpointSize * Integer.BYTES;
        content.position(checkpointStart);
        int lastCheckpoint = -1;
        for (int i = 0; i < checkpointSize; i++) {
            final int checkpoint = content.getInt();
            assert lastCheckpoint < checkpoint :
                    "checkpoint: " + checkpoint + ", lastCheckpoint: " + lastCheckpoint;
            lastCheckpoint = checkpoint;
            assert checkpoint < checkpointStart :
                    "checkpoint: " + checkpoint + ", checkpointStart: " + checkpointStart;
            this.checkpoints.add(checkpoint);
        }
        content.rewind();
        content.limit(checkpointStart);
    }

    @Override
    public SeekableIterator<Long, SerializedObjectWithId<byte[]>> iterator() {
        return new Itr(content);
    }

    private int findStartCheckpoint(long key) {
        int start = 0;
        int end = checkpoints.size();
        while (start < end - 1) {
            int mid = (start + end) / 2;

            content.position(checkpoints.get(mid));
            long k = content.getLong();

            if (key < k) {
                end = mid;
            } else if (key > k) {
                if (mid + 1 >= end) {
                    start = mid;
                } else {
                    content.position(checkpoints.get(mid + 1));
                    k = content.getLong();
                    if (key > k) {
                        start = mid + 1;
                    } else {
                        start = mid;
                        break;
                    }
                }
            } else {
                break;
            }
        }

        return start;
    }

    private byte[] readVal(ByteBuffer src, int len) {
        byte[] buffer = new byte[len];
        src.get(buffer);
        return buffer;
    }

    private class Itr implements SeekableIterator<Long, SerializedObjectWithId<byte[]>> {
        private final ByteBuffer content;
        private int offset;

        Itr(ByteBuffer content) {
            this.content = content;
        }

        @Override
        public SeekableIterator<Long, SerializedObjectWithId<byte[]>> seek(Long key) {
            final int checkpoint = findStartCheckpoint(key);
            offset = checkpoints.get(checkpoint);
            assert offset < content.limit();
            while (offset < content.limit()) {
                content.position(offset);
                final long k = content.getLong();
                final int len = content.getInt();
                assert len > 0;
                if (k <= key) {
                    offset += len + Long.BYTES + Integer.BYTES;
                } else {
                    break;
                }
            }
            return this;
        }

        @Override
        public boolean hasNext() {
            return offset < content.limit();
        }

        @Override
        public SerializedObjectWithId<byte[]> next() {
            assert offset < content.limit();

            content.position(offset);
            final long k = content.getLong();
            final int len = content.getInt();
            assert len > 0;
            final byte[] val = readVal(content, len);

            offset += len + Long.BYTES + Integer.BYTES;

            return new SerializedObjectWithId<>(k, val);
        }
    }
}
