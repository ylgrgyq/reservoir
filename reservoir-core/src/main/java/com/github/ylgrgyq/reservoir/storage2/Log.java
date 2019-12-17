package com.github.ylgrgyq.reservoir.storage2;

import com.github.ylgrgyq.reservoir.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public final class Log implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(Log.class.getSimpleName());

    private final String logName;
    private final Path logDirPath;
    private final ConcurrentNavigableMap<Long, LogSegment> segments;
    private final int segmentSize;
    @Nullable
    private LogSegment currentSegment;

    Log(Path baseDirPath, String logName, int maxSegmentSize) throws IOException {
        this.logName = logName;
        this.segmentSize = maxSegmentSize;
        this.logDirPath = baseDirPath.resolve(logName);
        FileUtils.forceMkdir(logDirPath.toFile());

        this.segments = new ConcurrentSkipListMap<>();

        recoverSegments();
        if (segments.lastEntry() != null) {
            this.currentSegment = segments.lastEntry().getValue();
        }
    }

    /**
     * Append the input group of {@link ByteBuffer}s into this Log.
     *
     * @param batch the input group of {@link ByteBuffer}s
     * @throws IOException if any I/O error occur
     */
    public void append(List<ByteBuffer> batch) throws IOException {
        LogSegment segment = currentSegment;
        if (segment == null || segment.size() >= segmentSize) {
            final long startId = segment == null ? 0 : segment.lastId() + 1;
            segment = addNewSegment(startId);
            currentSegment = segment;
        }
        segment.append(batch);
    }

    /**
     * Get the last id of this Log, inclusive; If the Log is empty, returns -1 instead.
     *
     * @return the last id of this segment
     */
    public long lastId() {
        final Map.Entry<Long, LogSegment> entry = segments.lastEntry();
        if (entry != null) {
            return entry.getValue().lastId();
        }
        return -1;
    }

    /**
     * Get a {@link FileRecords} slice contains a record with id equals to {@code fromId} from this Log.
     * If the {@code fromId} is not in this Log, {@code null} will be returned.
     * <p>
     * Note that if {@code fromId} exits in this Log, this method only insure that the returned
     * {@link FileRecords} contains the record with {@code fromId} but it may not be the first record
     * in this {@link FileRecords}.
     *
     * @param fromId the inclusive start id of the returned {@link FileRecords}
     * @return a {@link FileRecords} slice contains a record with the id equals to {@code fromId}
     * @throws IOException if any I/O error occur
     */
    @Nullable
    public FileRecords read(long fromId) throws IOException {
        final Entry<Long, LogSegment> floor = segments.floorEntry(fromId);
        if (floor != null && fromId <= floor.getValue().lastId()) {
            return floor.getValue().records(fromId);
        }

        final Entry<Long, LogSegment> ceiling = segments.ceilingEntry(fromId);
        if (ceiling != null) {
            return ceiling.getValue().records(fromId);
        }

        return null;
    }

    /**
     * Purge all the segments in this Log which can pass the test of the input {@link Predicate}.
     *
     * @param needPurge a {@link Predicate} to filter out segments to purge
     * @return how many segments was purged
     */
    int purgeSegments(Predicate<LogSegment> needPurge) {
        int purged = 0;
        for (Iterator<Entry<Long, LogSegment>> itr = segments.entrySet().iterator(); itr.hasNext(); ) {
            final Entry<Long, LogSegment> entry = itr.next();
            final LogSegment segment = entry.getValue();
            if (itr.hasNext()) {
                if (needPurge.test(segment)) {
                    itr.remove();
                    try {
                        segment.close();
                        Files.delete(segment.segmentFilePath());
                    } catch (IOException ex) {
                        logger.warn("Close segment {} failed.", segment, ex);
                    }
                    ++purged;
                }
            } else {
                // if this is the last segment, we do not purge it
                break;
            }
        }
        return purged;
    }

    @Override
    public void close() throws IOException {
        for (LogSegment segment : segments.values()) {
            segment.close();
        }
    }

    /**
     * Get how many segments are there in this Log
     *
     * @return the segments count in this Log
     */
    int size() {
        return segments.size();
    }

    private void recoverSegments() throws IOException {
        final File logBaseDirFile = logDirPath.toFile();
        final File[] files = logBaseDirFile.listFiles();
        if (files != null) {
            final List<Path> segmentPaths = Arrays.stream(files)
                    .filter(File::isFile)
                    .filter(f -> FileType.Segment.match(f.getName()))
                    .map(File::toPath)
                    .collect(Collectors.toList());

            for (Path segmentPath : segmentPaths) {
                try {
                    final LogSegment segment = LogSegment.fromFilePath(segmentPath);
                    addSegment(segment);
                } catch (EmptySegmentException ex) {
                    logger.info("Found empty log segment at path: " + segmentPath + ". This may caused by an unfinished" +
                            " write on this segment file. We can delete this file safely.");
                    Files.delete(segmentPath);
                }
            }

            long lastId = -1;
            for (LogSegment segment : segments.values()) {
                if (lastId < 0 || lastId + 1 == segment.startId()) {
                    lastId = segment.lastId();
                } else {
                    throw new InvalidSegmentException("non-consecutive segments found under log: " + logName + ". " +
                            "Last id in a segment is: " + lastId + ", but the start id of the next segment is: "
                            + segment.startId());
                }
            }
        }
    }

    private LogSegment addNewSegment(long startId) throws IOException {
        final String segmentName = SegmentFile.fileName(startId);
        return addSegment(LogSegment.newSegment(logDirPath.resolve(segmentName), startId));
    }

    private LogSegment addSegment(LogSegment segment) {
        final LogSegment old = segments.put(segment.startId(), segment);
        assert old == null;
        return segment;
    }
}
