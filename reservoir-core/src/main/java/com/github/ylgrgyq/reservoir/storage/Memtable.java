package com.github.ylgrgyq.reservoir.storage;

import com.github.ylgrgyq.reservoir.SerializedObjectWithId;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

final class Memtable implements Iterable<SerializedObjectWithId<byte[]>> {
    private final ConcurrentSkipListMap<Long, byte[]> table;
    private int memSize;

    Memtable() {
        table = new ConcurrentSkipListMap<>();
    }

    void add(SerializedObjectWithId<byte[]> val) {
        long k = val.getId();
        byte[] v = val.getSerializedObject();

        table.put(k, v);
        memSize += Long.BYTES + v.length;
    }

    long firstId() {
        if (table.isEmpty()) {
            return -1L;
        } else {
            return table.firstKey();
        }
    }

    long lastId() {
        if (table.isEmpty()) {
            return -1L;
        } else {
            return table.lastKey();
        }
    }

    boolean isEmpty() {
        return table.isEmpty();
    }

    List<SerializedObjectWithId<byte[]>> getEntries(long start, int limit) {
        if (start > lastId()) {
            return Collections.emptyList();
        }

        final SeekableIterator<Long, SerializedObjectWithId<byte[]>> iter = iterator();
        iter.seek(start);

        final List<SerializedObjectWithId<byte[]>> ret = new ArrayList<>();
        while (iter.hasNext()) {
            final SerializedObjectWithId<byte[]> v = iter.next();
            if (v.getId() > start && ret.size() < limit) {
                ret.add(v);
            } else {
                break;
            }
        }

        return ret;
    }

    int getMemoryUsedInBytes(){
        return memSize;
    }

    @Override
    public SeekableIterator<Long, SerializedObjectWithId<byte[]>> iterator() {
        return new Itr(table.clone());
    }

    private static class Itr implements SeekableIterator<Long, SerializedObjectWithId<byte[]>> {
        private final ConcurrentNavigableMap<Long, byte[]> innerMap;
        @Nullable
        private Map.Entry<Long, byte[]> offset;

        Itr(ConcurrentNavigableMap<Long, byte[]> innerMap) {
            this.innerMap = innerMap;
            this.offset = innerMap.firstEntry();
        }

        @Override
        public SeekableIterator<Long, SerializedObjectWithId<byte[]>> seek(Long key) {
            offset = innerMap.higherEntry(key);
            return this;
        }

        @Override
        public boolean hasNext() {
            return offset != null;
        }

        @Override
        public SerializedObjectWithId<byte[]> next() {
            if (offset == null) {
                throw new NoSuchElementException();
            }

            long id = offset.getKey();
            byte[] v = offset.getValue();
            offset = innerMap.higherEntry(id);
            return new SerializedObjectWithId<>(id, v);
        }
    }
}
