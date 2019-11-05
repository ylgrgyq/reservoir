package com.github.ylgrgyq.reservoir.storage2;

final class FileName {
    private static final String LOCK_FILE_PREFIX = "LOCK";

    static String getLockFileName() {
        return LOCK_FILE_PREFIX;
    }

    static String getLogSegmentFileName(String logName, long startId) {
        return logName + "-" + startId;
    }
}
