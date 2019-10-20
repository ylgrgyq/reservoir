package com.github.ylgrgyq.reservoir.benchmark;

import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

public final class DefaultBenchmarkReport implements BenchmarkTestReport {
    private final long testTimeElapsed;
    private final Timer timer;

    public DefaultBenchmarkReport(long testTimeElapsed, Timer operationTimer) {
        this.testTimeElapsed = testTimeElapsed;
        this.timer = operationTimer;
    }

    @Override
    public String report() {
        final Snapshot snapshot = timer.getSnapshot();
        return String.format("      time elapsed = %2.2f %s%n", convertDuration(testTimeElapsed), getDurationUnit()) +
                String.format("         mean rate = %2.2f calls/%s%n", timer.getMeanRate(), getRateUnit()) +
                String.format("     1-minute rate = %2.2f calls/%s%n", timer.getOneMinuteRate(), getRateUnit()) +
                String.format("               min = %2.2f %s%n", convertDuration(snapshot.getMin()), getDurationUnit()) +
                String.format("               max = %2.2f %s%n", convertDuration(snapshot.getMax()), getDurationUnit()) +
                String.format("              mean = %2.2f %s%n", convertDuration(snapshot.getMean()), getDurationUnit()) +
                String.format("            stddev = %2.2f %s%n", convertDuration(snapshot.getStdDev()), getDurationUnit()) +
                String.format("            median = %2.2f %s%n", convertDuration(snapshot.getMedian()), getDurationUnit()) +
                String.format("              75%% <= %2.2f %s%n", convertDuration(snapshot.get75thPercentile()), getDurationUnit()) +
                String.format("              95%% <= %2.2f %s%n", convertDuration(snapshot.get95thPercentile()), getDurationUnit()) +
                String.format("              98%% <= %2.2f %s%n", convertDuration(snapshot.get98thPercentile()), getDurationUnit()) +
                String.format("              99%% <= %2.2f %s%n", convertDuration(snapshot.get99thPercentile()), getDurationUnit()) +
                String.format("            99.9%% <= %2.2f %s%n", convertDuration(snapshot.get999thPercentile()), getDurationUnit());
    }

    @Override
    public String toString() {
        return report();
    }

    private String getDurationUnit() {
        return TimeUnit.MICROSECONDS.toString().toLowerCase(Locale.US);
    }

    private String getRateUnit() {
        final String s = TimeUnit.SECONDS.toString().toLowerCase(Locale.US);
        return s.substring(0, s.length() - 1);
    }

    private double convertDuration(double duration) {
        return duration / TimeUnit.MICROSECONDS.toNanos(1);
    }
}
