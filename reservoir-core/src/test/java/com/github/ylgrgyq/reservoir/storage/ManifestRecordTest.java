package com.github.ylgrgyq.reservoir.storage;

import org.assertj.core.util.Lists;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class ManifestRecordTest {
    @Test
    public void testEncodeEmptyMetas() {
        final ManifestRecord record = ManifestRecord.newPlainRecord();
        record.setDataLogFileNumber(101);
        record.setNextFileNumber(102);
        record.setConsumerCommitLogFileNumber(103);

        assertThat(ManifestRecord.decode(new CompositeBytesReader(Lists.list(record.encode())))).isEqualTo(record);
    }

    @Test
    public void testEncodeSomeMetas() {
        final ManifestRecord record = ManifestRecord.newPlainRecord();
        record.setDataLogFileNumber(101);
        record.setNextFileNumber(102);
        record.setConsumerCommitLogFileNumber(103);

        for (int i = 0; i < 1000; i++) {
            final SSTableFileMetaInfo meta = new SSTableFileMetaInfo();
            meta.setFileNumber(i);
            meta.setFileSize(i);
            meta.setFirstId(i);
            meta.setLastId(i);
            record.addMeta(meta);
        }

        assertThat(ManifestRecord.decode(new CompositeBytesReader(Lists.list(record.encode())))).isEqualTo(record);
    }

}