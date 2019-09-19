package com.github.ylgrgyq.reservoir.storage;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BlockHandleTest {

    @Test
    public void testEncodeDecode() {
        BlockIndexHandle handle = new BlockIndexHandle(Long.MIN_VALUE, Integer.MIN_VALUE);
        assertThat(BlockIndexHandle.decode(handle.encode())).isEqualTo(handle);
    }
}