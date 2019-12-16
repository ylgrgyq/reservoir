package com.github.ylgrgyq.reservoir.storage2;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * The schema is:
 * <p>
 * Length: Varint
 * Attributes: Int8
 * IdDelta: Varint
 * Value: Bytes
 * <p>
 * Please note that the Length field only includes Attributes, IdDelta, Value fields.
 * The size of the Length field itself is not added to Length.
 * <p>
 * But the {@code totalRecordSize} field in {@link DefaultRecord} is the total
 * size of the Record in bytes, which is equals to Length + bytes of Length in Varint.
 */
final class DefaultRecord implements Record {
    // We don't have any attributes now, so use a constant attributes
    final static byte DEFAULT_ATTRIBUTES = 0;

    static int writeTo(DataOutputStream out, int idDelta, ByteBuffer value) throws IOException {
        final int bodySizeInBytes = sizeOfBody(idDelta, value);
        ByteUtils.writeVarint(bodySizeInBytes, out);

        out.write(DEFAULT_ATTRIBUTES);

        ByteUtils.writeVarint(idDelta, out);

        final int valueSize = value.remaining();
        ByteUtils.writeVarint(valueSize, out);
        ByteUtils.writeTo(out, value, valueSize);
        return ByteUtils.sizeOfVarint(bodySizeInBytes) + bodySizeInBytes;
    }

    static DefaultRecord readFrom(ByteBuffer buffer, long baseId) {
        final int sizeOfBodyInBytes = ByteUtils.readVarint(buffer);
        if (buffer.remaining() < sizeOfBodyInBytes) {
            throw new BadRecordException("Found buffer underflow record with expected base id: " + baseId);
        }

        final int totalSizeInBytes = ByteUtils.sizeOfVarint(sizeOfBodyInBytes) + sizeOfBodyInBytes;
        return readFrom(buffer, totalSizeInBytes, sizeOfBodyInBytes, baseId);
    }

    static int calculateTotalRecordSize(int idDelta, ByteBuffer value) {
        int bodySize = sizeOfBody(idDelta, value);
        return bodySize + ByteUtils.sizeOfVarint(bodySize);
    }

    private static int sizeOfBody(int idDelta, ByteBuffer value) {
        int size = 1; // 1 byte for attributes
        size += ByteUtils.sizeOfVarint(idDelta);
        size += ByteUtils.sizeOfVarint(value.remaining());
        size += value.remaining();
        return size;
    }

    private static DefaultRecord readFrom(ByteBuffer buffer, int totalRecordSize, int recordBodySize, long baseId) {
        try {
            final int recordBodyStart = buffer.position();
            final byte attributes = buffer.get();
            final int idDelta = ByteUtils.readVarint(buffer);
            final long id = baseId + idDelta;
            final ByteBuffer value;
            final int valueSize = ByteUtils.readVarint(buffer);
            if (valueSize > 0) {
                value = buffer.slice();
                // may throw IllegalArgumentException if valueSize is larger
                // than the remaining size of this buffer
                value.limit(valueSize);
                buffer.position(buffer.position() + valueSize);
            } else {
                value = ByteBuffer.allocate(0);
            }

            if (buffer.position() - recordBodyStart != recordBodySize) {
                throw new BadRecordException("Invalid record size: expected to read " + recordBodySize +
                        " bytes but read " + (buffer.position() - recordBodyStart));
            }

            return new DefaultRecord(totalRecordSize, attributes, id, value);
        } catch (BufferUnderflowException | IllegalArgumentException e) {
            throw new BadRecordException("Found invalid record with expected base id: " + baseId, e);
        }
    }

    private final int totalRecordSize;
    private final byte attributes;
    private final long id;
    private final ByteBuffer value;

    private DefaultRecord(int totalRecordSize, byte attribute, long offset, ByteBuffer value) {
        this.totalRecordSize = totalRecordSize;
        this.attributes = attribute;
        this.id = offset;
        this.value = value;
    }

    @Override
    public int totalRecordSize() {
        return totalRecordSize;
    }

    @Override
    public long id() {
        return id;
    }

    @Override
    public ByteBuffer value() {
        return value;
    }

    @Override
    public int valueSize() {
        return value.remaining();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final DefaultRecord that = (DefaultRecord) o;
        return totalRecordSize == that.totalRecordSize &&
                attributes == that.attributes &&
                id == that.id &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(totalRecordSize, attributes, id, value);
    }

    @Override
    public String toString() {
        return "DefaultRecord{" +
                "id=" + id +
                ", value=" + value.limit() + " bytes" +
                '}';
    }
}
