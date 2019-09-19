package com.github.ylgrgyq.reservoir;

import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;

public final class SerializedObjectWithId<S> {
    private final long id;
    private final S serializedObject;

    /**
     * Create a new instance of SerializedObjectWithId.
     * For performance's sake, we do not clone the input {@code serializedObject},
     * so please mind not to modify the {@code serializedObject} after it has been
     * passed to this constructor when implementing {@link ObjectQueueStorage}.
     *
     * @param id               the ID of this {@code serializedObject} assigned by
     *                         {@link ObjectQueueStorage}
     * @param serializedObject the object in serialized form stored in
     *                         {@link ObjectQueueStorage}
     */
    public SerializedObjectWithId(long id, S serializedObject) {
        Objects.requireNonNull(serializedObject, "serializedObject");

        this.id = id;
        this.serializedObject = serializedObject;
    }

    /**
     * Get the assigned id for this object
     *
     * @return id of this object
     */
    public long getId() {
        return id;
    }

    /**
     * Get the serialized object.
     *
     * @return the serialized object
     */
    public S getSerializedObject() {
        return serializedObject;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final SerializedObjectWithId<?> that = (SerializedObjectWithId<?>) o;
        return getId() == that.getId() &&
                getSerializedObject().getClass() == that.getSerializedObject().getClass() &&
                (getSerializedObject() instanceof byte[] ?
                        Arrays.equals((byte[]) getSerializedObject(),
                                (byte[]) that.getSerializedObject()) :
                        getSerializedObject().equals(that.getSerializedObject()));
    }

    @Override
    public int hashCode() {
        final S obj = getSerializedObject();
        if (obj instanceof byte[]) {
            int result = Objects.hash(getId());
            result = 31 * result + Arrays.hashCode((byte[]) obj);
            return result;
        } else {
            return Objects.hash(getId(), obj);
        }
    }

    @Override
    public String toString() {
        return "SerializedObjectWithId{" +
                "id=" + id +
                ", serializedObject=" + (serializedObject instanceof byte[] ?
                Base64.getEncoder().encodeToString((byte[]) serializedObject) :
                serializedObject) +
                '}';
    }
}
