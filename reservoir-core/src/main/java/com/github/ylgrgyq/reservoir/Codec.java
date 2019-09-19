package com.github.ylgrgyq.reservoir;

/**
 * A codec to serialize and deserialize an instance between type T and type S.
 *
 * @param <T> the type of an object before serialization
 * @param <S> the type of an object after serialization. The most common type
 *            for S is byte[], but we decide not to constrain it to. In some
 *            scenarios this is more convenience like we can make S equals
 *            to T to save some codes in testing.
 */
public interface Codec<T, S> {
    /**
     * Serialize an object of type T to type S.
     *
     * @param obj an object to serialize
     * @return the serialized object of {@code obj}
     * @throws SerializationException when the object failed to serialize to type S.
     */
    S serialize(T obj) throws SerializationException;

    /**
     * Deserialize an object of type S to an object of type T.
     *
     * @param serializedObj an object to deserialize
     * @return the deserialized object of {@code serializedObj}
     * @throws DeserializationException when {@code serializedObj} failed to deserialize
     *                                  to the expect object of type T, like when
     *                                  S is byte[] and the input {@code serializedObj}
     *                                  is underflow.
     */
    T deserialize(S serializedObj) throws DeserializationException;
}
