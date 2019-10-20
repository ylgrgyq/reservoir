package com.github.ylgrgyq.reservoir.benchmark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public final class BenchmarkRunner {
    private static final Logger logger = LoggerFactory.getLogger(BenchmarkRunner.class.getSimpleName());

    private final int warmUpTimes;
    private final int testTimes;
    private final long coolDownIntervalMillis;

    public BenchmarkRunner(BenchmarkRunnerOptions options) {
        this.warmUpTimes = options.getWarmUpTimes();
        this.testTimes = options.getTestingTimes();
        this.coolDownIntervalMillis = TimeUnit.SECONDS.toMillis(options.getCoolDownSecs());
    }

    public void runTest(BenchmarkTest test) throws Exception {
        logger.info("\nEnvironment spec:\n{}\n", EnvironmentInfo.generateEnvironmentSpec());
        logger.info("\nTesting spec:\n{}\n", test.testingSpec());
        logger.info("Warm up for {} times.", warmUpTimes);
        doTest(test, warmUpTimes);

        logger.info("Test for {} times.", testTimes);
        doTest(test, testTimes);
    }

    private void doTest(BenchmarkTest test, int repeatTimes) throws Exception {
        for (int i = 1; i <= repeatTimes; i++) {
            test.setup();
            logger.info("The {} test start.", ordinalNumber(i));
            BenchmarkTestReport report = test.runTest();
            logger.info("The {} test done. Result: \n{}\n", ordinalNumber(i), report);
            test.teardown();

            // cool down a while except for after the last test
            if (i != repeatTimes) {
                Thread.sleep(coolDownIntervalMillis);
            }
        }
    }

    private String ordinalNumber(int i) {
        String[] sufixes = new String[]{"th", "st", "nd", "rd", "th", "th", "th", "th", "th", "th"};
        switch (i % 100) {
            case 11:
            case 12:
            case 13:
                return i + "th";
            default:
                return i + sufixes[i % 10];

        }
    }
}
