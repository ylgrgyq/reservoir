package com.github.ylgrgyq.reservoir.benchmark.storage;

import com.github.ylgrgyq.reservoir.SerializationException;
import com.github.ylgrgyq.reservoir.Verifiable;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

public class TestingPayload implements Verifiable {
    private static TestingPayloadCodec codec = new TestingPayloadCodec();

    private boolean valid;
    private byte[] content;

    public TestingPayload() {
        this.content = ("Hello").getBytes(StandardCharsets.UTF_8);
        this.valid = true;
    }

    public TestingPayload(byte[] content) {
        this.content = Arrays.copyOf(content, content.length);
        this.valid = true;
    }

    public TestingPayload(String contentString) {
        byte[] content = contentString.getBytes(StandardCharsets.UTF_8);
        this.content = Arrays.copyOf(content, content.length);
        this.valid = true;
    }

    public TestingPayload(boolean valid, byte[] content) {
        this.content = Arrays.copyOf(content, content.length);
        this.valid = valid;
    }

    public byte[] getContent() {
        return content;
    }

    public byte[] serialize() {
        try {
            return codec.serialize(this);
        } catch (SerializationException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public boolean isValid() {
        return valid;
    }

    public TestingPayload setValid(boolean valid) {
        this.valid = valid;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestingPayload payload = (TestingPayload) o;
        return Arrays.equals(getContent(), payload.getContent());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(getContent());
    }

    @Override
    public String toString() {
        return "TestingPayload{" +
                "content=" + Base64.getEncoder().encodeToString(content) +
                '}';
    }
}
