package com.github.ylgrgyq.reservoir.storage;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.ylgrgyq.reservoir.SerializedObjectWithId;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;

final class Table implements Iterable<SerializedObjectWithId<byte[]>> {
    static Table open(FileChannel fileChannel, long fileSize) throws IOException {
        final long footerOffset = fileSize - Footer.tableFooterSize;
        final ByteBuffer footerBuffer = ByteBuffer.allocate(Footer.tableFooterSize);
        fileChannel.read(footerBuffer, footerOffset);
        footerBuffer.flip();
        final Footer footer = Footer.decode(footerBuffer.array());
        final BlockIndexHandle blockIndexHandle = footer.getBlockIndexHandle();
        final Block indexBlock = readBlockFromChannel(fileChannel, blockIndexHandle);

        return new Table(fileChannel, indexBlock);
    }

    private static Block readBlockFromChannel(FileChannel fileChannel, BlockIndexHandle handle) throws IOException {
        final ByteBuffer content = ByteBuffer.allocate(handle.getBlockSize());
        fileChannel.position(handle.getBlockStartOffset());
        fileChannel.read(content);

        final ByteBuffer trailer = ByteBuffer.allocate(Constant.kBlockTrailerSize);
        fileChannel.read(trailer);
        trailer.flip();

        final long expectChecksum = trailer.getLong();
        final CRC32 actualChecksum = new CRC32();
        actualChecksum.update(content.array());
        if (expectChecksum != actualChecksum.getValue()) {
            throw new IllegalArgumentException("actualChecksum: " + actualChecksum.getValue()
                    + " (expect: = " + expectChecksum + ")");
        }

        return new Block(content);
    }

    private final Cache<Long, Block> dataBlockCache = Caffeine.newBuilder()
            .initialCapacity(1024)
            .maximumSize(2048)
            .build();
    private final FileChannel fileChannel;
    private final Block indexBlock;

    private Table(FileChannel fileChannel, Block indexBlock) {
        this.fileChannel = fileChannel;
        this.indexBlock = indexBlock;
    }

    void close() throws IOException {
        fileChannel.close();
        dataBlockCache.invalidateAll();
    }

    @Override
    public SeekableIterator<Long, SerializedObjectWithId<byte[]>> iterator() {
        return new Itr(indexBlock);
    }

    private class Itr implements SeekableIterator<Long, SerializedObjectWithId<byte[]>> {
        private final SeekableIterator<Long, SerializedObjectWithId<byte[]>> indexBlockIter;
        @Nullable
        private SeekableIterator<Long, SerializedObjectWithId<byte[]>> innerBlockIter;

        Itr(Block indexBlock) {
            this.indexBlockIter = indexBlock.iterator();
        }

        @Override
        public Itr seek(Long key) {
            indexBlockIter.seek(key);
            if (indexBlockIter.hasNext()) {
                innerBlockIter = createInnerBlockIter();
                innerBlockIter.seek(key);
            } else {
                innerBlockIter = null;
            }
            return this;
        }

        @Override
        public boolean hasNext() {
            if (innerBlockIter == null || !innerBlockIter.hasNext()) {
                if (indexBlockIter.hasNext()) {
                    innerBlockIter = createInnerBlockIter();
                }
            }

            return innerBlockIter != null && innerBlockIter.hasNext();
        }

        @Override
        public SerializedObjectWithId<byte[]> next() {
            assert innerBlockIter != null;
            assert innerBlockIter.hasNext();
            return innerBlockIter.next();
        }

        private SeekableIterator<Long, SerializedObjectWithId<byte[]>> createInnerBlockIter() {
            try {
                final SerializedObjectWithId<byte[]> kv = indexBlockIter.next();
                final BlockIndexHandle handle = BlockIndexHandle.decode(kv.getSerializedObject());
                final Block block = getBlock(handle);
                return block.iterator();
            } catch (IOException ex) {
                throw new StorageRuntimeException("create inner block iterator failed", ex);
            }
        }

        private Block getBlock(BlockIndexHandle handle) throws IOException {
            final Cache<Long, Block> cache = dataBlockCache;
            Block block = cache.getIfPresent(handle.getBlockStartOffset());
            if (block == null) {
                block = readBlockFromChannel(fileChannel, handle);
                cache.put(handle.getBlockStartOffset(), block);
            }

            return block;
        }
    }
}
