package com.github.ylgrgyq.reservoir.storage2;

import com.github.ylgrgyq.reservoir.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import static com.github.ylgrgyq.reservoir.storage2.StorageTestingUtils.generateSimpleTestingObjectWithIds;
import static com.github.ylgrgyq.reservoir.storage2.StorageTestingUtils.storeObjectWithIds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class FileStorage2Test {
    private String tempBaseDir;
    private FileStorage2 storage;

    @Before
    public void setUp() throws Exception {
        tempBaseDir = System.getProperty("java.io.tmpdir", "/tmp") +
                File.separator + "reservoir_test_" + System.nanoTime();
        storage = new FileStorage2(tempBaseDir);
    }

    @After
    public void tearDown() throws Exception {
        storage.close();
    }

    @Test
    public void twoFileStorageUsingSameWorkingDirectory() throws Exception {
        assertThatThrownBy(() -> new FileStorage2(tempBaseDir))
                .isInstanceOf(StorageException.class);
    }

    @Test
    public void commitThenGetCommitId() throws Exception {
        storage.commitId(100);
        assertThat(storage.getLastCommittedId()).isEqualTo(100);
        storage.close();
    }

    @Test
    public void blockFetch() throws Exception {
        final CyclicBarrier barrier = new CyclicBarrier(2);
        // block fetch in another thread
        final CompletableFuture<List<SerializedObjectWithId<byte[]>>> f = CompletableFuture.supplyAsync(() -> {
            try {
                barrier.await();
                return storage.fetch(Integer.MIN_VALUE, 100);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });

        // waiting fetch thread in position, then feed some data in storage
        barrier.await();
        final int expectSize = 64;
        final List<SerializedObjectWithId<byte[]>> objs = generateSimpleTestingObjectWithIds(expectSize);
        storeObjectWithIds(storage, objs);
        assertThat(f.get()).containsExactlyElementsOf(objs);
    }

    @Test
    public void blockFetchWithTimeout() throws Exception {
        assertThat(storage.fetch(0, 100, 100, TimeUnit.MILLISECONDS)).hasSize(0);
        storage.close();
    }

    @Test
    public void simpleProduceAndConsume() throws Exception {
        final ObjectQueue<TestingPayload> queue = ObjectQueueBuilder.newBuilder(storage, new TestingPayloadCodec())
                .buildQueue();
        final TestingPayload payload = new TestingPayload("first");
        queue.produce(payload);

        assertThat(queue.fetch()).isEqualTo(payload);
        queue.close();
    }
}