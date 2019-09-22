package com.github.ylgrgyq.reservoir.benchmark.storage;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;

final class EnvironmentInfo {
    private EnvironmentInfo() {}

    static String generateEnvironmentSpec() {
        return "OS: " + System.getProperty("os.name") + "\n" +
                "JDK: " + System.getProperty("java.version") + "\n" +
                "-Xmx: " + Runtime.getRuntime().maxMemory() / 1024 / 1024 + "MB\n" +
                "Garbage collector type: " + getGarbageCollectorType();
    }

    static String getGarbageCollectorType() {
        boolean appended = false;
        StringBuilder builder = new StringBuilder();
        List<GarbageCollectorMXBean> coll = ManagementFactory.getGarbageCollectorMXBeans();
        for (GarbageCollectorMXBean c : coll) {
            if (appended) {
                builder.append(" and ");
            }
            builder.append(c.getName());
            appended = true;
        }


        return builder.toString();
    }
}
