package com.github.ylgrgyq.reservoir;

public class BadTestingPayloadCodec<S> implements Codec<TestingPayload, S> {
    @Override
    public TestingPayload deserialize(S serializedObj) throws DeserializationException {
        throw new DeserializationException("deserialization failed");
    }

    @Override
    public S serialize(TestingPayload obj) throws SerializationException {
        throw new SerializationException("serialization failed");
    }
}
