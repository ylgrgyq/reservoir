package com.github.ylgrgyq.reservoir.storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

final class ManifestRecord {
    private final List<SSTableFileMetaInfo> metas;
    private final Type type;
    private int nextFileNumber;
    private int dataLogFileNumber;
    private int consumerCommitLogFileNumber;

    private ManifestRecord(Type type) {
        this.metas = new ArrayList<>();
        this.type = type;
    }

    static ManifestRecord newPlainRecord() {
        return new ManifestRecord(Type.PLAIN);
    }

    static ManifestRecord newReplaceAllExistedMetasRecord() {
        return new ManifestRecord(Type.REPLACE_METAS);
    }

    int getNextFileNumber() {
        if (type == Type.PLAIN) {
            return nextFileNumber;
        } else {
            return -1;
        }
    }

    void setNextFileNumber(int nextFileNumber) {
        assert type != Type.PLAIN || nextFileNumber > 1;
        this.nextFileNumber = nextFileNumber;
    }

    int getDataLogFileNumber() {
        if (type == Type.PLAIN) {
            return dataLogFileNumber;
        } else {
            return -1;
        }
    }

    int getConsumerCommitLogFileNumber() {
        if (type == Type.PLAIN) {
            return consumerCommitLogFileNumber;
        } else {
            return -1;
        }
    }

    void setDataLogFileNumber(int number) {
        assert type != Type.PLAIN || number > 0;
        this.dataLogFileNumber = number;
    }

    void setConsumerCommitLogFileNumber(int number) {
        assert type != Type.PLAIN || number > 0;
        this.consumerCommitLogFileNumber = number;
    }

    Type getType() {
        return type;
    }

    List<SSTableFileMetaInfo> getMetas() {
        return metas;
    }

    void addMeta(SSTableFileMetaInfo meta) {
        metas.add(meta);
    }

    void addMetas(List<SSTableFileMetaInfo> ms) {
        metas.addAll(ms);
    }

    byte[] encode() {
        final int sstableMetaInfoEncodeSize = Integer.BYTES + Long.BYTES * 3;
        final ByteBuffer buffer = ByteBuffer.allocate(1 + metas.size() * sstableMetaInfoEncodeSize + Integer.BYTES * 4);
        buffer.put(type.getCode());
        buffer.putInt(nextFileNumber);
        buffer.putInt(dataLogFileNumber);
        buffer.putInt(consumerCommitLogFileNumber);
        buffer.putInt(metas.size());

        for (SSTableFileMetaInfo meta : metas) {
            buffer.putLong(meta.getFileSize());
            buffer.putInt(meta.getFileNumber());
            buffer.putLong(meta.getFirstId());
            buffer.putLong(meta.getLastId());
        }

        return buffer.array();
    }

    private static byte[] compact(List<byte[]> output) {
        final int size = output.stream().mapToInt(b -> b.length).sum();
        final ByteBuffer buffer = ByteBuffer.allocate(size);
        for (byte[] bytes : output) {
            buffer.put(bytes);
        }
        return buffer.array();
    }

    static ManifestRecord decode(List<byte[]> bytes) {
        final ByteBuffer buffer = ByteBuffer.wrap(compact(bytes));
        final ManifestRecord record = Type.newManifestRecord(buffer.get());

        record.setNextFileNumber(buffer.getInt());
        record.setDataLogFileNumber(buffer.getInt());
        record.setConsumerCommitLogFileNumber(buffer.getInt());

        final int metasSize = buffer.getInt();
        for (int i = 0; i < metasSize; i++) {
            SSTableFileMetaInfo meta = new SSTableFileMetaInfo();
            meta.setFileSize(buffer.getLong());
            meta.setFileNumber(buffer.getInt());
            meta.setFirstId(buffer.getLong());
            meta.setLastId(buffer.getLong());

            record.addMeta(meta);
        }

        return record;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ManifestRecord that = (ManifestRecord) o;
        return getNextFileNumber() == that.getNextFileNumber() &&
                getDataLogFileNumber() == that.getDataLogFileNumber() &&
                getConsumerCommitLogFileNumber() == that.getConsumerCommitLogFileNumber() &&
                getMetas().equals(that.getMetas()) &&
                getType() == that.getType();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getMetas(), getType(), getNextFileNumber(),
                getDataLogFileNumber(), getConsumerCommitLogFileNumber());
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("ManifestRecord{" +
                "type=" + type +
                ", nextFileNumber=" + nextFileNumber +
                ", consumerCommitLogFileNumber=" + consumerCommitLogFileNumber +
                ", dataLogFileNumber=" + dataLogFileNumber);

        if (!metas.isEmpty()) {
            long from = metas.get(0).getFirstId();
            long to = metas.get(metas.size() - 1).getLastId();
            builder.append(", metaKeysFrom=");
            builder.append(from);
            builder.append(", metaKeysTo=");
            builder.append(to);
        }

        builder.append("}");

        return builder.toString();
    }

    enum Type {
        PLAIN((byte) 0),
        REPLACE_METAS((byte) 1);

        private byte code;

        Type(byte code) {
            this.code = code;
        }

        public byte getCode() {
            return code;
        }

        public static ManifestRecord newManifestRecord(byte typeCode) {
            switch (typeCode) {
                case 0:
                    return newPlainRecord();
                case 1:
                    return newReplaceAllExistedMetasRecord();
            }

            throw new IllegalArgumentException("unknown manifest record type code: " + typeCode);
        }
    }
}
