package com.github.ylgrgyq.reservoir.storage;

import com.github.ylgrgyq.reservoir.*;
import com.github.ylgrgyq.reservoir.storage.FileName.FileNameMeta;
import com.github.ylgrgyq.reservoir.storage.FileName.FileType;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.github.ylgrgyq.reservoir.TestingUtils.numberStringBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class FileStorageTest {
    private File tempFile;
    private FileStorageBuilder builder;

    @Before
    public void setUp() throws Exception {
        final String tempDir = System.getProperty("java.io.tmpdir", "/tmp") +
                File.separator + "reservoir_test_" + System.nanoTime();
        tempFile = new File(tempDir);
        FileUtils.forceMkdir(tempFile);
        builder = FileStorageBuilder.newBuilder(tempFile.getPath());
    }

    @Test
    public void commitThenGetCommitId() throws Exception {
        final FileStorage storage = builder.build();
        storage.commitId(100);
        assertThat(storage.getLastCommittedId()).isEqualTo(100);
        storage.close();
    }

    @Test
    public void consecutiveCommitThenGetCommitIdAfterRecoverUsingSameFile() throws Exception {
        FileStorage storage = builder.build();
        final FileNameMeta expectMeta = FileName.getFileNameMetas(tempFile.getPath(),
                meta -> meta.getType() == FileType.ConsumerCommit).get(0);

        for (int i = 1; i < 100; i++) {
            storage.commitId(i);
            storage.close();
            storage = builder.build();
            assertThat(storage.getLastCommittedId()).isEqualTo(i);
        }
        assertThat(FileName.getFileNameMetas(tempFile.getPath(), meta -> meta.getType() == FileType.ConsumerCommit))
                .hasSize(1).allMatch(meta -> meta.getFileNumber() == expectMeta.getFileNumber());
        storage.close();
    }

    @Test
    public void blockFetch() throws Exception {
        final FileStorage storage = builder.build();
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
        assertThat(storage.getLastProducedId()).isEqualTo(expectSize - 1);
        assertThat(f.get()).containsExactlyElementsOf(objs);
        storage.close();
    }

    @Test
    public void blockFetchWithTimeout() throws Exception {
        final FileStorage storage = builder.build();

        assertThat(storage.fetch(0, 100, 100, TimeUnit.MILLISECONDS)).hasSize(0);
        storage.close();
    }

    @Test
    public void fetchDataFromMemtable() throws Exception {
        final FileStorage storage = builder.build();
        final int expectSize = 64;

        final List<SerializedObjectWithId<byte[]>> objs = generateSimpleTestingObjectWithIds(expectSize);
        storeObjectWithIds(storage, objs);
        assertThat(storage.getLastProducedId()).isEqualTo(expectSize - 1);

        for (int i = -1; i < expectSize; i += 10) {
            assertThat(storage.fetch(i, 100)).isEqualTo(objs.subList(i + 1, objs.size()));
        }
        storage.close();
    }

    @Test
    public void fetchDataFromRecoveredMemtable() throws Exception {
        FileStorage storage = builder.build();
        final int expectSize = 64;

        final List<SerializedObjectWithId<byte[]>> objs = generateSimpleTestingObjectWithIds(expectSize);
        storeObjectWithIds(storage, objs);
        storage.close();

        storage = builder.build();
        assertThat(storage.getLastProducedId()).isEqualTo(expectSize - 1);
        assertThat(storage.fetch(-1, 100)).isEqualTo(objs);
        storage.close();
    }

    @Test
    public void testWorkingWithCleanDirectory() throws Exception {
        FileStorage storage = builder.build();
        final int expectSize = 64;

        final List<SerializedObjectWithId<byte[]>> objs = generateSimpleTestingObjectWithIds(expectSize);
        storeObjectWithIds(storage, objs);
        storage.close();

        storage = builder.startWithCleanDirectory(true).build();
        assertThat(storage.getLastProducedId()).isEqualTo(-1);
        storage.close();
    }

    @Test
    public void fetchDataFromImmutableMemtable() throws Exception {
        final FileStorage storage = builder
                // block flush memtable job, so immutable memtable will stay in mem during the test
                .flushMemtableExecutorService(new DelayedSingleThreadExecutorService(1, TimeUnit.DAYS))
                .build();

        final List<SerializedObjectWithId<byte[]>> objs = generateSimpleTestingObjectWithIds(64);
        storeObjectWithIds(storage, objs);

        triggerFlushMemtable(storage);
        assertThat(storage.fetch(-1, 64)).isEqualTo(objs);
        storage.close(true);
    }

    @Test
    public void fetchDataOnlyFromImmutableMemtableAndMemtable() throws Exception {
        final FileStorage storage = builder
                // block flush memtable job, so immutable memtable will stay in mem during the test
                .flushMemtableExecutorService(new DelayedSingleThreadExecutorService(1, TimeUnit.DAYS))
                .build();

        final List<SerializedObjectWithId<byte[]>> objs = generateSimpleTestingObjectWithIds(64);
        storeObjectWithIds(storage, objs);

        objs.addAll(triggerFlushMemtable(storage));
        assertThat(storage.fetch(31, 1000)).isEqualTo(objs.subList(32, objs.size()));
        storage.close(true);
    }


    @Test
    public void fetchDataFromRecoveredSstable() throws Exception {
        FileStorage storage = builder
                // block flush memtable job, so immutable memtable will stay in mem during the test
                .flushMemtableExecutorService(new DelayedSingleThreadExecutorService(1, TimeUnit.DAYS))
                .build();

        final List<SerializedObjectWithId<byte[]>> objs = generateSimpleTestingObjectWithIds(64);
        storeObjectWithIds(storage, objs);
        triggerFlushMemtable(storage);
        storage.close(true);
        storage = builder
                // create executor service again, because the previous executor service in the storage
                // builder is closed when the previous storage closed
                .flushMemtableExecutorService(new DelayedSingleThreadExecutorService(1, TimeUnit.DAYS))
                .build();
        assertThat(storage.fetch(-1, 64)).isEqualTo(objs);
        storage.close(true);
    }

    @Test
    public void fetchDataFromSstableImmutableMemtableAndMemtable() throws Exception {
        final FileStorage storage = builder
                // allow only one flush task finish immediately
                .flushMemtableExecutorService(new DelayedSingleThreadExecutorService(1, 1, TimeUnit.DAYS))
                .build();

        // 1. write some data
        final List<SerializedObjectWithId<byte[]>> expectData = generateSimpleTestingObjectWithIds(64);
        storeObjectWithIds(storage, expectData);
        // 2. flush for the first time, make every data write before flushed to sstable
        expectData.addAll(triggerFlushMemtable(storage));
        // 3. write some more data
        final List<SerializedObjectWithId<byte[]>> objInMem = generateSimpleTestingObjectWithIds(storage.getLastProducedId() + 1, 128);
        expectData.addAll(objInMem);
        storeObjectWithIds(storage, objInMem);
        // 4. flush again, but this time the flush task will be blocked so
        // every data write in step 3 will be stay in immutable table and the data triggering
        // the flush task will stay in memtable
        expectData.addAll(triggerFlushMemtable(storage));

        // add one to limit to ensure there's no more data in storage than in expectData
        assertThat(storage.fetch(-1, expectData.size() + 1)).isEqualTo(expectData);
        storage.close(true);
    }

    @Test
    public void fetchDataFromMultiSstables() throws Exception {
        final FileStorage storage = builder
                .flushMemtableExecutorService(new ImmediateExecutorService())
                .build();

        final List<SerializedObjectWithId<byte[]>> expectData = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            List<SerializedObjectWithId<byte[]>> batch = generateSimpleTestingObjectWithIds(storage.getLastProducedId() + 1, 64);
            expectData.addAll(batch);
            storeObjectWithIds(storage, batch);
            expectData.addAll(triggerFlushMemtable(storage));
        }

        assertThat(storage.fetch(-1, expectData.size() + 1)).isEqualTo(expectData);
        storage.close();
    }

    @Test
    public void testWriteImmutableTableBlock() throws Exception {
        final DelayedSingleThreadExecutorService executorService = new DelayedSingleThreadExecutorService(1, TimeUnit.DAYS);
        final FileStorage storage = builder
                .flushMemtableExecutorService(executorService)
                .build();

        // 1. trigger immutable table flush
        final List<SerializedObjectWithId<byte[]>> expectData = generateSimpleTestingObjectWithIds(storage.getLastProducedId() + 1, 64);
        storeObjectWithIds(storage, expectData);
        expectData.addAll(triggerFlushMemtable(storage));

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CompletableFuture<Void> f = CompletableFuture.runAsync(() -> {
            try {
                barrier.await();
                // 2. trigger another immutable table flush, this one will block until the first one finish
                expectData.addAll(triggerFlushMemtable(storage));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });

        barrier.await();
        // 3. release the first immutable table flush
        executorService.letOnePass();
        // the second immutable table flush is not complete yet
        assertThat(f.isDone()).isFalse();
        // 4. release the second immutable table flush
        executorService.letOnePass();
        // 5. wait the second immutable table flush task done
        f.join();
        assertThat(storage.fetch(-1, Integer.MAX_VALUE)).containsExactlyElementsOf(expectData);

        storage.close(true);
    }

    @Test
    // add this to pass null to writeMemtable to throw exception
    @SuppressWarnings("ConstantConditions")
    public void testFlushMemtableFailedThenStorageClosed() throws Exception {
        final FileStorage storage = builder
                .flushMemtableExecutorService(new ImmediateExecutorService())
                .build();

        storage.writeMemtable(null);
        await().until(storage::closed);
    }

    @Test
    public void truncate() throws Exception {
        final FileStorage storage = builder
                .flushMemtableExecutorService(new ImmediateExecutorService())
                .truncateIntervalMillis(0, TimeUnit.MILLISECONDS)
                .build();
        final int expectSize = 65;
        final List<SerializedObjectWithId<byte[]>> objs = generateSimpleTestingObjectWithIds(expectSize);
        storeObjectWithIds(storage, objs);
        objs.addAll(triggerFlushMemtable(storage));
        storage.commitId(1000);
        objs.add(new SerializedObjectWithId<>(1010101, "Trigger truncate".getBytes(StandardCharsets.UTF_8)));

        final List<SerializedObjectWithId<byte[]>> actualObjs = storage.fetch(0, 100);
        assertThat(actualObjs.iterator().next().getId()).isGreaterThan(1);

        storage.close();
    }

    @Test
    public void simpleProduceAndConsume() throws Exception {
        final FileStorage storage = builder.build();
        final ObjectQueue<TestingPayload> queue = ObjectQueueBuilder.newBuilder(storage, new TestingPayloadCodec())
                .buildQueue();
        final TestingPayload payload = new TestingPayload("first");
        queue.produce(payload);

        assertThat(queue.fetch()).isEqualTo(payload);
        queue.close();
    }

    private List<SerializedObjectWithId<byte[]>> generateSimpleTestingObjectWithIds(int expectSize) {
        return generateSimpleTestingObjectWithIds(0L, expectSize);
    }

    private List<SerializedObjectWithId<byte[]>> generateSimpleTestingObjectWithIds(long startId, int expectSize) {
        final List<SerializedObjectWithId<byte[]>> objs = new ArrayList<>();
        for (long i = startId; i < startId + expectSize; i++) {
            final SerializedObjectWithId<byte[]> obj = new SerializedObjectWithId<>(i, numberStringBytes(i));
            objs.add(obj);
        }
        return objs;
    }

    private void storeObjectWithIds(FileStorage storage, List<SerializedObjectWithId<byte[]>> batch) throws StorageException {
        storage.store(batch.stream().map(SerializedObjectWithId::getSerializedObject).collect(Collectors.toList()));
    }

    private List<SerializedObjectWithId<byte[]>> triggerFlushMemtable(FileStorage storage) throws StorageException {
        long nextId = storage.getLastProducedId() + 1;
        List<SerializedObjectWithId<byte[]>> triggerDatas = Arrays.asList(
                new SerializedObjectWithId<>(nextId, new byte[Constant.kMaxMemtableSize]),
                new SerializedObjectWithId<>(nextId + 1, numberStringBytes(nextId + 1)));
        storeObjectWithIds(storage, triggerDatas);
        return triggerDatas;
    }
}