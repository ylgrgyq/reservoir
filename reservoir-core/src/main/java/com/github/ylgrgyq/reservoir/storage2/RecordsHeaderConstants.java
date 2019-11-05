package com.github.ylgrgyq.reservoir.storage2;

final class RecordsHeaderConstants {
    private RecordsHeaderConstants() {}

    /**
     * The current "magic" value
     */
    static final byte CURRENT_MAGIC_VALUE = 0;

    static final int BASE_ID_OFFSET = 0;
    static final int BASE_ID_LENGTH = 8;
    static final int SIZE_OFFSET = BASE_ID_OFFSET + BASE_ID_LENGTH;
    static final int SIZE_LENGTH = 4;
    static final int BASIC_HEADER_OVERHEAD = SIZE_OFFSET + SIZE_LENGTH;

    static final int MAGIC_OFFSET = BASIC_HEADER_OVERHEAD;
    static final int MAGIC_LENGTH = 1;
    static final int HEADER_SIZE_UP_TO_MAGIC = MAGIC_OFFSET + MAGIC_LENGTH;
}
