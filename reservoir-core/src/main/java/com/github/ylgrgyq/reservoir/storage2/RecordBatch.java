package com.github.ylgrgyq.reservoir.storage2;


public interface RecordBatch extends Iterable<Record> {
    byte magic();

    long baseId();

    long lastIdDelta();

    long lastId();

    long nextId();

    int totalBatchSize();

    int count();

    long checksum();

    boolean isValid();
}
