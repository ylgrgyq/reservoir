package com.github.ylgrgyq.reservoir.storage2;

import java.util.StringTokenizer;

final class Java {
    private Java() { }

    private static final Version VERSION = parseVersion(System.getProperty("java.specification.version"));

    // Package private for testing
    static Version parseVersion(String versionString) {
        final StringTokenizer st = new StringTokenizer(versionString, ".");
        final int majorVersion = Integer.parseInt(st.nextToken());
        final int minorVersion;
        if (st.hasMoreTokens()) {
            minorVersion = Integer.parseInt(st.nextToken());
        } else {
            minorVersion = 0;
        }
        return new Version(majorVersion, minorVersion);
    }

    // Having these as static final provides the best opportunity for compilar optimization
    static final boolean IS_JAVA9_COMPATIBLE = VERSION.isJava9Compatible();

    // Package private for testing
    static class Version {
        final int majorVersion;
        final int minorVersion;

        private Version(int majorVersion, int minorVersion) {
            this.majorVersion = majorVersion;
            this.minorVersion = minorVersion;
        }

        @Override
        public String toString() {
            return "Version(majorVersion=" + majorVersion +
                    ", minorVersion=" + minorVersion + ")";
        }

        // Package private for testing
        boolean isJava9Compatible() {
            return majorVersion >= 9;
        }
    }
}
