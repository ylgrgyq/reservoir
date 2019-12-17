package com.github.ylgrgyq.reservoir.storage2;

final class SegmentFile implements Comparable<SegmentFile> {
    private static final FileType type = FileType.Segment;

    static SegmentFile fromPath(String fileName) {
        assert type.match(fileName);
        String startIdStr = fileName.substring(
                type.prefix().length() + 1,
                fileName.length() - (type.suffix() == null ? 0 : type.suffix().length()));
        long startId = Long.valueOf(startIdStr);
        return new SegmentFile(startId);
    }

    static String fileName(long startId) {
        return type.prefix() + "-" + startId + type.suffix();
    }

    private final long startId;

    private SegmentFile(long startId) {
        this.startId = startId;
    }

    long startId() {
        return startId;
    }

    @Override
    public int compareTo(SegmentFile o) {
        return Long.compare(startId, o.startId);
    }
}
