package com.github.ylgrgyq.reservoir.storage;

import com.github.ylgrgyq.reservoir.storage.FileName.FileNameMeta;
import com.github.ylgrgyq.reservoir.storage.FileName.FileType;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class FileNameTest {

    @Test
    public void testParseManifestFile() {
        String fileName = FileName.getManifestFileName(100);
        FileNameMeta meta = FileName.parseFileName(fileName);
        assertThat(meta.getFileName()).isEqualTo(fileName);
        assertThat(meta.getFileNumber()).isEqualTo(100);
        assertThat(meta.getType()).isEqualTo(FileType.Manifest);
    }

}