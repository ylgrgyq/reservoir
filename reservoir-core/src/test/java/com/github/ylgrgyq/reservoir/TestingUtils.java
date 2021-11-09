package com.github.ylgrgyq.reservoir;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Random;

public class TestingUtils {
    public static String numberString(Number num) {
        return "" + num;
    }

    public static byte[] numberStringBytes(Number num) {
        return ("" + num).getBytes(StandardCharsets.UTF_8);
    }

    public static ByteBuffer numberStringByteBuffer(Number num) {
        return ByteBuffer.wrap(("" + num).getBytes(StandardCharsets.UTF_8));
    }

    @SuppressWarnings("StatementWithEmptyBody")
    public static int nextPositiveInt(Random random, int bound) {
        int nextInt;
        while ((nextInt = random.nextInt(bound)) <= 0) {
            // loop
        }
        return nextInt;
    }

    public static String makeString(String base, int expectSize) {
        final int baseInNeed = expectSize / base.length();
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < baseInNeed; i++) {
            builder.append(base);
        }

        builder.append(base, 0, expectSize - baseInNeed * base.length());
        return builder.toString();
    }
}
