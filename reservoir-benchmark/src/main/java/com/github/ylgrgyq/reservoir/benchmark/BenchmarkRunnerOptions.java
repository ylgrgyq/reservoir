package com.github.ylgrgyq.reservoir.benchmark;


import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command()
public class BenchmarkRunnerOptions {
    @Option(names = {"-w", "--warm-up-times"},
            defaultValue = "5",
            description = "Warm-up testing times before the start of the official tests.")
    private int warmUpTimes;

    @Option(names = {"-t", "--testing-times"},
            defaultValue = "3",
            description = "Official testing times after warm-up period.")
    private int testingTimes;

    @Option(names = {"-c", "--cool-down-interval-secs"},
            defaultValue = "5",
            description = "Cool down interval in seconds between each tests.")
    private int coolDownSecs;

    public int getWarmUpTimes() {
        return warmUpTimes;
    }

    public int getTestingTimes() {
        return testingTimes;
    }

    public int getCoolDownSecs() {
        return coolDownSecs;
    }
}
