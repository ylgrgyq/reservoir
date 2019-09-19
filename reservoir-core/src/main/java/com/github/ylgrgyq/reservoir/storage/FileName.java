package com.github.ylgrgyq.reservoir.storage;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

final class FileName {
    private static final String CURRENT_FILE_PREFIX = "CURRENT";
    private static final String LOCK_FILE_PREFIX = "LOCK";
    private static final String LOG_FILE_PREFIX = "LOG";
    private static final String MANIFEST_FILE_PREFIX = "MANIFEST";
    private static final String CONSUMER_COMMITTED_ID_FILE_PREFIX = "CONSUMER";

    static String getCurrentFileName() {
        return CURRENT_FILE_PREFIX;
    }

    private static String generateFileName(String prefix, int fileNumber, String suffix) {
        return String.format("%s-%07d.%s", prefix, fileNumber, suffix);
    }

    private static String generateFileName(int fileNumber, String suffix) {
        return String.format("%07d.%s", fileNumber, suffix);
    }

    static String getLockFileName() {
        return LOCK_FILE_PREFIX;
    }

    static String getSSTableName(int fileNumber) {
        return generateFileName(fileNumber, "sst");
    }

    static String getLogFileName(int fileNumber) {
        return generateFileName(LOG_FILE_PREFIX, fileNumber, "log");
    }

    static String getConsumerCommittedIdFileName(int fileNumber) {
        return generateFileName(CONSUMER_COMMITTED_ID_FILE_PREFIX, fileNumber, "commit");
    }

    static String getManifestFileName(int fileNumber) {
        return generateFileName(MANIFEST_FILE_PREFIX, fileNumber, "mf");
    }

    static void setCurrentFile(String baseDir, int manifestFileNumber) throws IOException {
        Path tmpPath = Files.createTempFile(Paths.get(baseDir), "CURRENT_TMP", ".tmp_mf");
        try {
            String manifestFileName = getManifestFileName(manifestFileNumber);

            Files.write(tmpPath, manifestFileName.getBytes(StandardCharsets.UTF_8), StandardOpenOption.SYNC,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE);

            Files.move(tmpPath, Paths.get(baseDir, getCurrentFileName()), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException ex) {
            Files.deleteIfExists(tmpPath);
            throw ex;
        }
    }

    static FileNameMeta parseFileName(String fileName) {
        if (fileName.startsWith("/") || fileName.startsWith("./")) {
            String[] strs = fileName.split("/");
            assert strs.length > 0;
            fileName = strs[strs.length - 1];
        }

        FileType type = FileType.Unknown;
        int fileNumber = 0;
        if (fileName.endsWith(CURRENT_FILE_PREFIX)) {
            type = FileType.Current;
        } else if (fileName.endsWith(".lock")) {
            type = FileType.Lock;
        } else if (fileName.endsWith(".tmp_mf")) {
            type = FileType.TempManifest;
        } else {
            String[] strs = fileName.split("[\\-.]", 3);
            if (strs.length == 3) {
                fileNumber = Integer.parseInt(strs[1]);
                switch (strs[2]) {
                    case "sst":
                        type = FileType.SSTable;
                        break;
                    case "log":
                        type = FileType.Log;
                        break;
                    case "mf":
                        type = FileType.Manifest;
                        break;
                    case "commit":
                        type = FileType.ConsumerCommit;
                        break;
                    default:
                        break;
                }
            }
        }
        return new FileNameMeta(fileName, fileNumber, type);
    }

    static List<FileNameMeta> getFileNameMetas(String baseDir, Predicate<? super FileNameMeta> filter) {
        final File baseDirFile = new File(baseDir);
        final File[] files = baseDirFile.listFiles();
        List<FileNameMeta> consumerLogFileMetas = Collections.emptyList();
        if (files != null) {
            consumerLogFileMetas = Arrays.stream(files)
                    .filter(File::isFile)
                    .map(File::getName)
                    .map(FileName::parseFileName)
                    .filter(filter)
                    .sorted(Comparator.comparingInt(FileNameMeta::getFileNumber))
                    .collect(Collectors.toList());
        }

        return consumerLogFileMetas;
    }

    static class FileNameMeta {
        private final String fileName;
        private final int fileNumber;
        private final FileType type;

        FileNameMeta(String fileName, int fileNumber, FileType type) {
            this.fileName = fileName;
            this.fileNumber = fileNumber;
            this.type = type;
        }

        String getFileName() {
            return fileName;
        }

        int getFileNumber() {
            return fileNumber;
        }

        FileType getType() {
            return type;
        }
    }

    enum FileType {
        Unknown,
        SSTable,
        Current,
        Log,
        Manifest,
        ConsumerCommit,
        TempManifest,
        Lock
    }
}
