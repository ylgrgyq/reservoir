package com.github.ylgrgyq.reservoir.benchmark.storage;

import com.github.ylgrgyq.reservoir.FileUtils;
import com.github.ylgrgyq.reservoir.ObjectQueue;
import com.github.ylgrgyq.reservoir.ObjectQueueBuilder;
import com.github.ylgrgyq.reservoir.SerializedObjectWithId;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class RocksDbStorageTest {
    private File tempFile;

    @Before
    public void setUp() throws Exception {
        String tempDir = System.getProperty("java.io.tmpdir", "/tmp") +
                File.separator + "reservoir_test_" + System.nanoTime();
        tempFile = new File(tempDir);
        FileUtils.forceMkdir(tempFile);
    }

    @Test
    public void commitId() throws Exception {
        RocksDbStorage storage = new RocksDbStorage(tempFile.getPath(), true);
        storage.commitId(100);
        assertThat(storage.getLastCommittedId()).isEqualTo(100);
        storage.close();
    }

    @Test
    public void simpleStore() throws Exception {
        RocksDbStorage storage = new RocksDbStorage(tempFile.getPath(), true);
        final int expectSize = 64;
        List<SerializedObjectWithId<byte[]>> objs = new ArrayList<>();
        for (int i = 1; i < expectSize + 1; i++) {
            SerializedObjectWithId<byte[]> obj = new SerializedObjectWithId<>(i, ("" + i).getBytes(StandardCharsets.UTF_8));
            objs.add(obj);
        }
        storeObjectWithId(storage, objs);
        assertThat(storage.getLastProducedId()).isEqualTo(expectSize);
        assertThat(objs)
                .containsExactlyElementsOf(storage.fetch(0, 100));
        storage.close();
    }

    @Test
    public void blockFetch() throws Exception {
        RocksDbStorage storage = new RocksDbStorage(tempFile.getPath(), true, 50);
        CyclicBarrier barrier = new CyclicBarrier(2);
        CompletableFuture<List<SerializedObjectWithId<byte[]>>> f = CompletableFuture.supplyAsync(() -> {
            try {
                barrier.await();
                return storage.fetch(0, 100);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });

        barrier.await();
        final int expectSize = 64;
        List<SerializedObjectWithId<byte[]>> objs = new ArrayList<>();
        for (int i = 1; i < expectSize + 1; i++) {
            SerializedObjectWithId<byte[]> obj = new SerializedObjectWithId<>(i, ("" + i).getBytes(StandardCharsets.UTF_8));
            objs.add(obj);
        }
        storeObjectWithId(storage, objs);
        assertThat(storage.getLastProducedId()).isEqualTo(expectSize);
        assertThat(objs)
                .containsExactlyElementsOf(f.get());
        storage.close();
    }

    @Test
    public void blockFetchTimeout() throws Exception {
        RocksDbStorage storage = new RocksDbStorage(tempFile.getPath(), true);

        assertThat(storage.fetch(0, 100, 100, TimeUnit.MILLISECONDS)).hasSize(0);
        storage.close();
    }

    @Test
    public void truncate() throws Exception {
        RocksDbStorage storage = new RocksDbStorage(tempFile.getPath(), true, 500, 100);
        final int expectSize = 2000;
        List<SerializedObjectWithId<byte[]>> objs = new ArrayList<>();
        for (int i = 1; i < expectSize + 1; i++) {
            SerializedObjectWithId<byte[]> obj = new SerializedObjectWithId<>(i, TestingUtils.numberStringBytes(i));
            objs.add(obj);
        }
        storeObjectWithId(storage, objs);
        storage.commitId(2000);
        await().until(() -> {
            List<SerializedObjectWithId<byte[]>> actualObjs = storage.fetch(0, 100);
            return actualObjs.iterator().next().getId() == 1000;
        });
        storage.close();
    }

    @Test
    public void simpleProduceAndConsume() throws Exception {
        final RocksDbStorage storage = new RocksDbStorage(tempFile.getPath(), true);
        final ObjectQueue<TestingPayload> queue = ObjectQueueBuilder.newBuilder(storage, new TestingPayloadCodec())
                .buildQueue();
        final TestingPayload payload = new TestingPayload("first");
        queue.produce(payload);

        assertThat(queue.fetch()).isEqualTo(payload);
        queue.close();
    }

    private void storeObjectWithId(RocksDbStorage storage, List<SerializedObjectWithId<byte[]>> batch) {
        storage.store(batch.stream().map(SerializedObjectWithId::getSerializedObject).collect(Collectors.toList()));
    }
}