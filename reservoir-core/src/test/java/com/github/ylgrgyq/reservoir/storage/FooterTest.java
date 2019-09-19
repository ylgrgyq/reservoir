package com.github.ylgrgyq.reservoir.storage;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FooterTest {
    @Test
    public void testGetIndexBlockHandle() {
        final BlockIndexHandle blockIndexHandle = new BlockIndexHandle(Long.MAX_VALUE, 1010101);
        final Footer footer = new Footer(blockIndexHandle);
        assertThat(footer.getBlockIndexHandle()).isEqualTo(blockIndexHandle);
    }

    @Test
    public void testEncodeDecode() {
        final BlockIndexHandle blockIndexHandle = new BlockIndexHandle(Long.MAX_VALUE, 1010101);
        final Footer expectFooter = new Footer(blockIndexHandle);
        final Footer actualFooter = Footer.decode(expectFooter.encode());
        assertThat(actualFooter).isEqualTo(expectFooter);

        assertThat(actualFooter.getBlockIndexHandle()).isEqualTo(blockIndexHandle);
    }
}