package com.github.ylgrgyq.reservoir.storage2;

import java.nio.ByteBuffer;

public interface Record {
    long id();

    int totalRecordSize();

    int valueSize();

    ByteBuffer value();
}
