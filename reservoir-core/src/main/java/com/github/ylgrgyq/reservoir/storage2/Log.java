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
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class Log implements Closeable {
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

    public void append(List<ByteBuffer> batch) throws IOException {
        LogSegment segment = currentSegment;
        if (segment == null || segment.size() >= segmentSize) {
            final long startId = segment == null ? 0 : segment.lastId() + 1;
            segment = addNewSegment(startId);
            currentSegment = segment;
        }
        segment.append(batch);
    }

    public long lastId() {
        return segments.lastEntry().getValue().lastId();
    }

    /**
     * @param fromId inclusive
     * @return
     * @throws IOException
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

    public void purgeOutdatedSegments(Predicate<LogSegment> needPurge) {
        for (Iterator<Entry<Long, LogSegment>> itr = segments.entrySet().iterator(); itr.hasNext(); ) {
            final Entry<Long, LogSegment> entry = itr.next();
            final LogSegment segment = entry.getValue();
            if (itr.hasNext()) {
                if (needPurge.test(segment)) {
                    itr.remove();
                    try {
                        segment.close();
                    } catch (IOException ex) {
                        logger.warn("Close segment {} failed.", segment, ex);
                    }
                }
            } else {
                // if this is the last segment, we do not purge it
                break;
            }
        }
    }

    @Override
    public void close() throws IOException {
        for (LogSegment segment : segments.values()) {
            segment.close();
        }
    }

    int size() {
        return segments.size();
    }

    private void recoverSegments() throws IOException {
        final File logBaseDirFile = logDirPath.toFile();
        final File[] files = logBaseDirFile.listFiles();
        if (files != null) {
            final List<Path> segmentPaths = Arrays.stream(files)
                    .filter(File::isFile)
                    .filter(f -> FileType.parseFileType(f.getName()) == FileType.Segment)
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
                    throw new InvalidSegmentException("non-consecutive segments found under " + logName + ". " +
                            "Last id in the last segment is: " + lastId + ", but the next segment is start at: "
                            + segment.startId());
                }
            }
        }
    }

    private LogSegment addNewSegment(long startId) throws IOException {
        final String segmentName = FileName.getLogSegmentFileName(startId);
        return addSegment(LogSegment.newSegment(logDirPath.resolve(segmentName), startId));
    }

    private LogSegment addSegment(LogSegment segment) {
        final LogSegment old = segments.put(segment.startId(), segment);
        assert old == null;
        return segment;
    }
}
