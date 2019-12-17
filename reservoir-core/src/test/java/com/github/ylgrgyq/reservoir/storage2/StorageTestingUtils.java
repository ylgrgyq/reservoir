package com.github.ylgrgyq.reservoir.storage2;

import com.github.ylgrgyq.reservoir.SerializedObjectWithId;
import com.github.ylgrgyq.reservoir.StorageException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.github.ylgrgyq.reservoir.TestingUtils.numberStringByteBuffer;
import static com.github.ylgrgyq.reservoir.TestingUtils.numberStringBytes;

class StorageTestingUtils {
    static MemoryRecords generateMemoryRecords(long baseId, int batchSize) throws IOException {
        final List<ByteBuffer> batch = generateTestingBytes(batchSize);
        return new MemoryRecords(baseId, batch);
    }

    static List<ByteBuffer> generateTestingBytes(int expectSize) {
        final List<ByteBuffer> batch = new ArrayList<>();
        for (long i = 0; i < expectSize; i++) {
            final ByteBuffer bs = numberStringByteBuffer(i);
            batch.add(bs);
        }
        return batch;
    }

    static List<SerializedObjectWithId<byte[]>> generateSimpleTestingObjectWithIds(int expectSize) {
        return generateSimpleTestingObjectWithIds(0L, expectSize);
    }

    static List<SerializedObjectWithId<byte[]>> generateSimpleTestingObjectWithIds(long startId, int expectSize) {
        final List<SerializedObjectWithId<byte[]>> objs = new ArrayList<>();
        for (long i = startId; i < startId + expectSize; i++) {
            final SerializedObjectWithId<byte[]> obj = new SerializedObjectWithId<>(i, numberStringBytes(i));
            objs.add(obj);
        }
        return objs;
    }

    static void storeObjectWithIds(FileStorage2 storage, List<SerializedObjectWithId<byte[]>> batch) throws StorageException {
        storage.store(batch.stream().map(SerializedObjectWithId::getSerializedObject).collect(Collectors.toList()));
    }
}
