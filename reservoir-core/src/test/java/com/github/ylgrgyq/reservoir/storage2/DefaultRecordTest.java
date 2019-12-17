package com.github.ylgrgyq.reservoir.storage2;

import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static com.github.ylgrgyq.reservoir.storage2.DefaultRecord.DEFAULT_ATTRIBUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DefaultRecordTest {
    private static final long baseId = 10101;

    @Test
    public void testWriteAndReadRecord() throws IOException {
        final ByteBuffer value = ByteBuffer.wrap("Hello".getBytes(StandardCharsets.UTF_8));
        final ByteBufferOutputStream out = new ByteBufferOutputStream();
        final int idDelta = 512;
        final int written = DefaultRecord.writeTo(new DataOutputStream(out), idDelta, value);
        assertThat(written).isEqualTo(DefaultRecord.calculateTotalRecordSize(idDelta, value));

        ByteBuffer buffer = out.buffer().duplicate();
        buffer.flip();
        final DefaultRecord record = DefaultRecord.readFrom(buffer, baseId);
        assertThat(record.id()).isEqualTo(baseId + idDelta);
        assertThat(record.value()).isEqualTo(value);
        assertThat(record.totalRecordSize()).isEqualTo(DefaultRecord.calculateTotalRecordSize(idDelta, value));
    }

    @Test
    public void testWriteAndReadEmptyValueRecord() throws IOException {
        final ByteBuffer value = ByteBuffer.allocate(0);
        final ByteBufferOutputStream out = new ByteBufferOutputStream();
        final int idDelta = 512;
        final int written = DefaultRecord.writeTo(new DataOutputStream(out), idDelta, value);
        assertThat(written).isEqualTo(DefaultRecord.calculateTotalRecordSize(idDelta, value));

        ByteBuffer buffer = out.buffer().duplicate();
        buffer.flip();
        final DefaultRecord record = DefaultRecord.readFrom(buffer, baseId);
        assertThat(record.id()).isEqualTo(baseId + idDelta);
        assertThat(record.value()).isEqualTo(value);
        assertThat(record.totalRecordSize()).isEqualTo(DefaultRecord.calculateTotalRecordSize(idDelta, value));
    }

    @Test
    public void testInvalidValueSize() {
        final int idDelta = 512;
        final int sizeOfBody = 100;

        ByteBuffer buffer = ByteBuffer.allocate(sizeOfBody + ByteUtils.sizeOfVarint(sizeOfBody));
        ByteUtils.writeVarint(sizeOfBody, buffer);
        buffer.put(DEFAULT_ATTRIBUTES);
        ByteUtils.writeVarint(idDelta, buffer);
        // write a large size but no that much value in buffer
        ByteUtils.writeVarint(1000, buffer);
        buffer.position(buffer.limit());

        buffer.flip();
        assertThatThrownBy(() -> DefaultRecord.readFrom(buffer, baseId))
                .isInstanceOf(BadRecordException.class)
                .hasMessageContaining("Found invalid record with expected base id");
    }

    @Test
    public void testBufferUnderflow() {
        final int sizeOfBody = 100;

        ByteBuffer buffer = ByteBuffer.allocate(10);
        ByteUtils.writeVarint(sizeOfBody, buffer);

        buffer.flip();

        assertThatThrownBy(() -> DefaultRecord.readFrom(buffer, baseId))
                .isInstanceOf(BadRecordException.class)
                .hasMessageContaining("Found buffer underflow record with expected base id");
    }

    @Test
    public void testInvalidRecordSize() throws IOException{
        final int idDelta = 512;
        final int sizeOfBody = 100;
        final ByteBuffer value = ByteBuffer.wrap("Hello".getBytes(StandardCharsets.UTF_8));

        ByteBuffer buffer = ByteBuffer.allocate(sizeOfBody + ByteUtils.sizeOfVarint(sizeOfBody));
        // write a large body size, but the actual value size is smaller than it
        ByteUtils.writeVarint(sizeOfBody, buffer);
        buffer.put(DEFAULT_ATTRIBUTES);
        ByteUtils.writeVarint(idDelta, buffer);
        ByteUtils.writeVarint(value.remaining(), buffer);
        ByteUtils.writeTo(buffer, value, value.remaining());
        buffer.position(buffer.limit());

        buffer.flip();
        assertThatThrownBy(() -> DefaultRecord.readFrom(buffer, baseId))
                .isInstanceOf(BadRecordException.class)
                .hasMessageContaining("Invalid record size: expected to read");
    }
}