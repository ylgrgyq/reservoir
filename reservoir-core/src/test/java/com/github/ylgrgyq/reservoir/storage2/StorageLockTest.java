package com.github.ylgrgyq.reservoir.storage2;

import com.github.ylgrgyq.reservoir.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class StorageLockTest {
    private Path tempBaseDirPath;
    private StorageLock lock;

    @Before
    public void setUp() throws Exception {
        tempBaseDirPath = Paths.get(System.getProperty("java.io.tmpdir", "/tmp") +
                File.separator + "reservoir_test_" + System.nanoTime());
        FileUtils.forceMkdir(tempBaseDirPath.toFile());
        lock = new StorageLock(tempBaseDirPath);
        lock.lock();
    }

    @After
    public void tearDown() throws Exception {
        lock.unlock();
    }

    @Test
    public void twoLockOnSameDirectory() {
        assertThatThrownBy(() -> new StorageLock(tempBaseDirPath).lock())
                .isInstanceOf(OverlappingFileLockException.class);
    }

    @Test
    public void lockTwice() throws Exception {
        assertThat(lock.isLocked()).isTrue();
        lock.lock();
        assertThat(lock.isLocked()).isTrue();
    }

    @Test
    public void unlockBeforeLock() throws Exception {
        assertThat(lock.isLocked()).isTrue();
        lock.unlock();
        assertThat(lock.isLocked()).isFalse();
    }

    @Test
    public void lockAfterUnlock() throws Exception{
        assertThat(lock.isLocked()).isTrue();
        lock.unlock();
        assertThat(lock.isLocked()).isFalse();
        lock.lock();
        assertThat(lock.isLocked()).isTrue();
    }
}