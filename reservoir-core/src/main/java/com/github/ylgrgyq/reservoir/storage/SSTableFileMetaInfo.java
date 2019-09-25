package com.github.ylgrgyq.reservoir.storage;

import java.util.Objects;

final class SSTableFileMetaInfo {
    static final int SERIALIZED_SIZE = Integer.BYTES + Long.BYTES + Long.BYTES + Long.BYTES;

    private int fileNumber;
    private long firstId;
    private long lastId;
    private long fileSize;

    void setFileNumber(int fileNumber) {
        assert fileNumber >= 0;
        this.fileNumber = fileNumber;
    }

    void setFirstId(long firstIndex) {
        assert firstIndex >= 0;
        this.firstId = firstIndex;
    }

    void setLastId(long lastIndex) {
        assert lastIndex >= 0;
        this.lastId = lastIndex;
    }

    void setFileSize(long fileSize) {
        assert fileSize >= 0;
        this.fileSize = fileSize;
    }

    int getFileNumber() {
        return fileNumber;
    }

    long getFirstId() {
        return firstId;
    }

    long getLastId() {
        return lastId;
    }

    long getFileSize() {
        return fileSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SSTableFileMetaInfo)) return false;
        SSTableFileMetaInfo that = (SSTableFileMetaInfo) o;
        return getFileNumber() == that.getFileNumber() &&
                getFirstId() == that.getFirstId() &&
                getLastId() == that.getLastId() &&
                getFileSize() == that.getFileSize();
    }

    @Override
    public int hashCode() {

        return Objects.hash(getFileNumber(), getFirstId(), getLastId(), getFileSize());
    }

    @Override
    public String toString() {
        return "SSTableFileMetaInfo{" +
                "fileNumber=" + fileNumber +
                ", firstId=" + firstId +
                ", lastId=" + lastId +
                ", fileSize=" + fileSize +
                '}';
    }
}
