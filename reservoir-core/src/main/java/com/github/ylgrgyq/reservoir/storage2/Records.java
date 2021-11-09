package com.github.ylgrgyq.reservoir.storage2;

public interface Records {

    int totalSizeInBytes();

    Iterable<? extends RecordBatch> batches();

    Iterable<? extends Record> records();
}
