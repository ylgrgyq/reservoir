package com.github.ylgrgyq.reservoir;

import java.nio.ByteBuffer;

public class TestingPayloadCodec implements Codec<TestingPayload, byte[]> {
    private static final int MINIMUM_LENGTH = Integer.BYTES + 1;

    @Override
    public TestingPayload deserialize(byte[] serializedObj) throws DeserializationException {
        if (serializedObj.length < MINIMUM_LENGTH) {
            throw new DeserializationException("buffer underflow, at least needs "
                    + MINIMUM_LENGTH + " bytes, actual: " + serializedObj.length);
        }

        ByteBuffer buffer = ByteBuffer.wrap(serializedObj);
        boolean valid = buffer.get() == (byte) 1;
        int len = buffer.getInt();
        byte[] content = new byte[len];
        buffer.get(content);

        return new TestingPayload(valid, content);
    }


    @Override
    public byte[] serialize(TestingPayload obj) throws SerializationException {
        byte[] bs = new byte[MINIMUM_LENGTH + obj.getContent().length];
        ByteBuffer buffer = ByteBuffer.wrap(bs);
        buffer.put(obj.isValid() ? (byte) 1 : (byte) 0);
        buffer.putInt(obj.getContent().length);
        buffer.put(obj.getContent());
        return buffer.array();
    }
}
