package com.github.ylgrgyq.reservoir.storage2;

import javax.annotation.Nullable;
import java.util.Iterator;

public abstract class AbstractRecords implements Records {
    private final Iterable<Record> records = this::recordsIterator;

    @Override
    public Iterable<Record> records() {
        return records;
    }

    private Iterator<Record> recordsIterator() {
        return new AbstractIterator<Record>() {
            private final Iterator<? extends RecordBatch> batches = batches().iterator();
            @Nullable
            private Iterator<Record> records;

            @Override
            protected Record makeNext() {
                if (records != null && records.hasNext())
                    return records.next();

                if (batches.hasNext()) {
                    records = batches.next().iterator();
                    return makeNext();
                }

                return allDone();
            }
        };
    }

}
