package com.github.ylgrgyq.reservoir.benchmark;

public interface BenchmarkTest {
    default void setup() throws Exception {}

    default void teardown() throws Exception {}

    BenchmarkTestReport runTest() throws Exception;

    String testingSpec();
}
