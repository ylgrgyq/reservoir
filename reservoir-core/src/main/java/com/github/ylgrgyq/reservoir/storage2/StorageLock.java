package com.github.ylgrgyq.reservoir.storage2;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

final class StorageLock {
    private static final FileType type = FileType.Lock;

    private final Path storageDirPath;
    @Nullable
    private FileLock fileLock;

    StorageLock(Path storageBaseDirPath) {
        this.storageDirPath = storageBaseDirPath;
    }

    boolean isLocked() {
        return fileLock != null && fileLock.isValid();
    }

    void lock() throws IOException {
        if (isLocked()) {
            return;
        }

        final Path lockFilePath = storageDirPath.resolve(type.prefix());
        final FileChannel lockChannel = FileChannel.open(lockFilePath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        try {
            fileLock = lockChannel.tryLock();
            if (fileLock == null) {
                throw new IllegalStateException("failed to lock directory: " + storageDirPath);
            }
        } finally {
            if (fileLock == null || !fileLock.isValid()) {
                lockChannel.close();
            }
        }
    }

    void unlock() throws IOException {
        if (fileLock != null) {
            try (Channel channel = fileLock.acquiredBy()) {
                if (channel.isOpen()) {
                    fileLock.release();
                    fileLock = null;
                }
            }
        }
    }
}
