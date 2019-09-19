package com.github.ylgrgyq.reservoir.storage;

import com.github.ylgrgyq.reservoir.FileUtils;
import com.github.ylgrgyq.reservoir.StorageException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

public class ManifestTest {
    private File testingBaseDir;
    private Manifest manifest;

    @Before
    public void setUp() throws Exception {
        String tempDir = System.getProperty("java.io.tmpdir", "/tmp") +
                File.separator + "reservoir_test_" + System.nanoTime();
        testingBaseDir = new File(tempDir);
        FileUtils.forceMkdir(testingBaseDir);
        manifest = new Manifest(testingBaseDir.getPath());
    }

    @After
    public void tearDown() throws Exception {
        manifest.close();
    }

    @Test
    public void testGetFirstId() throws Exception {
        assertThat(manifest.getFirstId()).isEqualTo(-1);

        for (int i = 2; i < 100; i++) {
            final ManifestRecord record = ManifestRecord.newPlainRecord();
            final SSTableFileMetaInfo meta = new SSTableFileMetaInfo();
            meta.setFirstId(i);
            record.addMeta(meta);
            manifest.logRecord(record);

            assertThat(manifest.getFirstId()).isEqualTo(2);
        }
    }

    @Test
    public void testGetLastId() throws Exception {
        assertThat(manifest.getLastId()).isEqualTo(-1);

        for (int i = 2; i < 100; i++) {
            final ManifestRecord record = ManifestRecord.newPlainRecord();
            final SSTableFileMetaInfo meta = new SSTableFileMetaInfo();
            meta.setLastId(i);
            record.addMeta(meta);
            manifest.logRecord(record);

            assertThat(manifest.getLastId()).isEqualTo(i);
        }
    }

    @Test
    public void testGetNextFileNumber() {
        assertThat(manifest.getNextFileNumber()).isEqualTo(1);
        for (int i = 2; i < 10000; i++) {
            assertThat(manifest.getNextFileNumber()).isEqualTo(i);
        }
    }

    @Test
    public void testGetDataLogNumber() throws Exception {
        assertThat(manifest.getDataLogFileNumber()).isEqualTo(0);

        for (int i = 2; i < 100; i++) {
            final ManifestRecord record = ManifestRecord.newPlainRecord();
            record.setDataLogFileNumber(i);
            manifest.logRecord(record);

            assertThat(manifest.getDataLogFileNumber()).isEqualTo(i);
        }
    }

    @Test
    public void testLogPlainRecord() throws Exception {
        final List<SSTableFileMetaInfo> expectMetas = new ArrayList<>();
        final ManifestRecord emptyMetaRecord = newPlainManifestRecord(2);
        manifest.logRecord(emptyMetaRecord);

        ManifestRecord record = newPlainManifestRecord(3);
        SSTableFileMetaInfo meta = newSStableFileMetaInfo(3);
        expectMetas.add(meta);
        record.addMeta(meta);
        manifest.logRecord(record);

        record = newPlainManifestRecord(4);
        meta = newSStableFileMetaInfo(4);
        expectMetas.add(meta);
        record.addMeta(meta);
        manifest.logRecord(record);

        List<SSTableFileMetaInfo> metas = getAllMetas(manifest);
        assertThat(metas).isEqualTo(expectMetas);
    }

    @Test
    public void testLogReplaceMetaRecord() throws Exception {
        final ManifestRecord emptyMetaRecord = newPlainManifestRecord(2);
        manifest.logRecord(emptyMetaRecord);

        ManifestRecord record = newPlainManifestRecord(3);
        record.addMeta(newSStableFileMetaInfo(3));
        manifest.logRecord(record);

        record = newReplaceMetasManifestRecord(4);
        List<SSTableFileMetaInfo> metas = newSStableFileMetaInfosWithNumber(4, 5, 6, 7);
        final List<SSTableFileMetaInfo> expectMetas = new ArrayList<>(metas);
        metas.forEach(record::addMeta);
        manifest.logRecord(record);

        record = newPlainManifestRecord(8);
        SSTableFileMetaInfo meta = newSStableFileMetaInfo(8);
        expectMetas.add(meta);
        record.addMeta(meta);
        manifest.logRecord(record);

        List<SSTableFileMetaInfo> actualMetas = getAllMetas(manifest);
        assertThat(actualMetas).isEqualTo(expectMetas);
    }

    @Test
    public void testRecoverFromNonExistManifest() {
        String tempDir = "recover_from_non_exists_" + System.nanoTime();
        File tempFile = new File(tempDir);
        assertThatThrownBy(() -> manifest.recover(tempFile.getPath()))
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("CURRENT file points to an non-exists manifest file");
    }

    @Test
    public void testRecoverWithPlainMetaRecords() throws Exception {
        final ManifestRecord emptyMetaRecord = newPlainManifestRecord(2);
        manifest.logRecord(emptyMetaRecord);

        ManifestRecord record = newPlainManifestRecord(3);
        SSTableFileMetaInfo meta = newSStableFileMetaInfo(3);
        record.addMeta(meta);
        manifest.logRecord(record);

        record = newPlainManifestRecord(4);
        meta = newSStableFileMetaInfo(4);
        record.addMeta(meta);
        manifest.logRecord(record);

        Manifest recoveredManifest = new Manifest(testingBaseDir.getPath());
        recoveredManifest.recover(getCurrentManifestFileName());
        assertThat(recoveredManifest).isEqualTo(manifest);
    }

    @Test
    public void testRecoverWithReplaceMetaRecords() throws Exception {
        final ManifestRecord emptyMetaRecord = newPlainManifestRecord(2);
        manifest.logRecord(emptyMetaRecord);

        ManifestRecord record = newPlainManifestRecord(3);
        record.addMeta(newSStableFileMetaInfo(3));
        manifest.logRecord(record);

        record = newReplaceMetasManifestRecord(4);
        List<SSTableFileMetaInfo> metas = newSStableFileMetaInfosWithNumber(4, 5, 6, 7);
        metas.forEach(record::addMeta);
        manifest.logRecord(record);

        record = newPlainManifestRecord(8);
        SSTableFileMetaInfo meta = newSStableFileMetaInfo(8);
        record.addMeta(meta);
        manifest.logRecord(record);

        Manifest recoveredManifest = new Manifest(testingBaseDir.getPath());
        recoveredManifest.recover(getCurrentManifestFileName());
        assertThat(recoveredManifest).isEqualTo(manifest);
    }

    @Test
    public void testSearchMetas() throws Exception {
        final ManifestRecord record = newPlainManifestRecord(2);
        final List<SSTableFileMetaInfo> metas = new ArrayList<>();
        metas.add(newSStableFileMetaInfo(10, 100));
        metas.add(newSStableFileMetaInfo(101, 500));
        metas.add(newSStableFileMetaInfo(501, 1000));
        metas.add(newSStableFileMetaInfo(1001, 2000));
        metas.forEach(record::addMeta);
        manifest.logRecord(record);

        assertThat(manifest.searchMetas(0)).isEqualTo(metas);
        assertThat(manifest.searchMetas(10)).isEqualTo(metas);
        assertThat(manifest.searchMetas(50)).isEqualTo(metas);
        assertThat(manifest.searchMetas(100)).isEqualTo(metas);

        assertThat(manifest.searchMetas(101)).isEqualTo(metas.subList(1,  metas.size()));
        assertThat(manifest.searchMetas(400)).isEqualTo(metas.subList(1,  metas.size()));
        assertThat(manifest.searchMetas(500)).isEqualTo(metas.subList(1,  metas.size()));

        assertThat(manifest.searchMetas(501)).isEqualTo(metas.subList(2,  metas.size()));
        assertThat(manifest.searchMetas(900)).isEqualTo(metas.subList(2,  metas.size()));
        assertThat(manifest.searchMetas(1000)).isEqualTo(metas.subList(2,  metas.size()));

        assertThat(manifest.searchMetas(1001)).hasSize(1).containsExactly(metas.get(metas.size() - 1));
        assertThat(manifest.searchMetas(2000)).hasSize(1).containsExactly(metas.get(metas.size() - 1));

        assertThat(manifest.searchMetas(3000)).isEmpty();
    }

    @Test
    public void testTruncateNothing() throws Exception {
        final List<SSTableFileMetaInfo> metas = prepareTruncateTest();

        manifest.truncateToId(0);
        assertThat(getAllMetas(manifest)).isEqualTo(metas);
        manifest.truncateToId(10);
        assertThat(getAllMetas(manifest)).isEqualTo(metas);
        manifest.truncateToId(100);
        assertThat(getAllMetas(manifest)).isEqualTo(metas);
    }

    @Test
    public void testTruncateSome() throws Exception {
        final List<SSTableFileMetaInfo> metas = prepareTruncateTest();

        manifest.truncateToId(101);
        assertThat(getAllMetas(manifest)).isEqualTo(metas.subList(1, metas.size()));
        Manifest recoveredManifest = new Manifest(testingBaseDir.getPath());
        recoveredManifest.recover(getCurrentManifestFileName());
        assertThat(getAllMetas(recoveredManifest)).isEqualTo(metas.subList(1, metas.size()));
        assertThat(recoveredManifest).isEqualTo(manifest);
    }

    @Test
    public void testTruncateSome2() throws Exception {
        final List<SSTableFileMetaInfo> metas = prepareTruncateTest();

        manifest.truncateToId(500);
        assertThat(getAllMetas(manifest)).isEqualTo(metas.subList(1, metas.size()));
        Manifest recoveredManifest = new Manifest(testingBaseDir.getPath());
        recoveredManifest.recover(getCurrentManifestFileName());
        assertThat(getAllMetas(recoveredManifest)).isEqualTo(metas.subList(1, metas.size()));
        assertThat(recoveredManifest).isEqualTo(manifest);
    }

    @Test
    public void testTruncateSome3() throws Exception {
        final List<SSTableFileMetaInfo> metas = prepareTruncateTest();

        manifest.truncateToId(501);
        assertThat(getAllMetas(manifest)).isEqualTo(metas.subList(2, metas.size()));
        Manifest recoveredManifest = new Manifest(testingBaseDir.getPath());
        recoveredManifest.recover(getCurrentManifestFileName());
        assertThat(getAllMetas(recoveredManifest)).isEqualTo(metas.subList(2, metas.size()));
        assertThat(recoveredManifest).isEqualTo(manifest);
    }

    @Test
    public void testTruncateAll() throws Exception {
        prepareTruncateTest();

        manifest.truncateToId(2001);
        assertThat(getAllMetas(manifest)).isEmpty();
        Manifest recoveredManifest = new Manifest(testingBaseDir.getPath());
        recoveredManifest.recover(getCurrentManifestFileName());
        assertThat(recoveredManifest).isEqualTo(manifest);
    }

    private ManifestRecord newPlainManifestRecord(int number) {
        final ManifestRecord record = ManifestRecord.newPlainRecord();
        record.setDataLogFileNumber(number);
        record.setConsumerCommitLogFileNumber(number);
        record.setNextFileNumber(number);
        return record;
    }

    private ManifestRecord newReplaceMetasManifestRecord(int number) {
        final ManifestRecord record = ManifestRecord.newReplaceAllExistedMetasRecord();
        record.setDataLogFileNumber(number);
        record.setConsumerCommitLogFileNumber(number);
        record.setNextFileNumber(number);
        return record;
    }

    private SSTableFileMetaInfo newSStableFileMetaInfo(int number) {
        SSTableFileMetaInfo meta = new SSTableFileMetaInfo();
        meta.setLastId(number);
        meta.setFirstId(number);
        meta.setFileSize(10101);
        meta.setFileNumber(number);
        return meta;
    }

    private SSTableFileMetaInfo newSStableFileMetaInfo(int firstId, int lastId) {
        SSTableFileMetaInfo meta = new SSTableFileMetaInfo();
        meta.setLastId(lastId);
        meta.setFirstId(firstId);
        meta.setFileSize(10101);
        meta.setFileNumber(firstId);
        return meta;
    }

    private List<SSTableFileMetaInfo> newSStableFileMetaInfosWithNumber(Integer... numbers) {
        final List<SSTableFileMetaInfo> list = new ArrayList<>();
        for (int number : numbers) {
            SSTableFileMetaInfo meta = new SSTableFileMetaInfo();
            meta.setLastId(number);
            meta.setFirstId(number);
            meta.setFileSize(10101);
            meta.setFileNumber(number);
            list.add(meta);
        }
        return list;
    }

    private String getCurrentManifestFileName() throws IOException {
        final Path currentFilePath = Paths.get(testingBaseDir.getPath(), FileName.getCurrentFileName());
        return new String(Files.readAllBytes(currentFilePath), StandardCharsets.UTF_8);
    }

    private List<SSTableFileMetaInfo> getAllMetas(Manifest manifest) {
        return manifest.searchMetas(Long.MIN_VALUE);
    }

    private List<SSTableFileMetaInfo> prepareTruncateTest() throws Exception{
        final ManifestRecord record = newPlainManifestRecord(2);
        final List<SSTableFileMetaInfo> metas = new ArrayList<>();
        metas.add(newSStableFileMetaInfo(10, 100));
        metas.add(newSStableFileMetaInfo(101, 500));
        metas.add(newSStableFileMetaInfo(501, 1000));
        metas.add(newSStableFileMetaInfo(1001, 2000));
        metas.forEach(record::addMeta);
        manifest.logRecord(record);
        return metas;
    }
}