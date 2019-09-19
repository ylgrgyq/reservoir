package com.github.ylgrgyq.reservoir.storage;

import com.github.ylgrgyq.reservoir.storage.BlockBuilder.WriteBlockResult;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

final class TableBuilder {
    private final FileChannel fileChannel;
    private final BlockBuilder pendingDataBlock;
    private final BlockBuilder indexBlock;
    private long lastKey;
    private long nextBlockOffset;
    private boolean isFinished;
    @Nullable
    private BlockIndexHandle pendingBlockIndexHandle;

    TableBuilder(FileChannel fileChannel) {
        this.fileChannel = fileChannel;
        this.pendingDataBlock = new BlockBuilder();
        this.indexBlock = new BlockBuilder();
        this.lastKey = -1;
    }

    void add(long k, byte[] v) throws IOException {
        assert k > lastKey;
        assert v.length > 0;
        assert !isFinished;

        if (pendingBlockIndexHandle != null) {
            indexBlock.add(k, pendingBlockIndexHandle.encode());
            pendingBlockIndexHandle = null;
        }

        pendingDataBlock.add(k, v);

        if (pendingDataBlock.estimateBlockSize() >= Constant.kMaxDataBlockSize) {
            flushDataBlock();
        }

        lastKey = k;
    }

    long finishBuild() throws IOException {
        assert ! isFinished;
        // nextBlockOffset can equal to 0 when no data block has been written
        assert nextBlockOffset >= 0;

        isFinished = true;

        if (!pendingDataBlock.isEmpty()) {
            flushDataBlock();
        }

        if (pendingBlockIndexHandle != null) {
            indexBlock.add(lastKey + 1, pendingBlockIndexHandle.encode());
            pendingBlockIndexHandle = null;
        }

        final BlockIndexHandle blockIndexHandle = writeBlock(indexBlock);
        final Footer footer = new Footer(blockIndexHandle);
        final byte[] footerBytes = footer.encode();
        fileChannel.write(ByteBuffer.wrap(footerBytes));

        return nextBlockOffset + footerBytes.length;
    }

    private void flushDataBlock() throws IOException {
        assert ! pendingDataBlock.isEmpty();
        pendingBlockIndexHandle = writeBlock(pendingDataBlock);
        fileChannel.force(true);
    }

    private BlockIndexHandle writeBlock(BlockBuilder block) throws IOException{
        assert fileChannel.position() == nextBlockOffset :
                "position: " + fileChannel.position() + " nextBlockOffset: " + nextBlockOffset;

        final long blockStartOffset = nextBlockOffset;
        final WriteBlockResult result = block.writeBlock(fileChannel);
        final long checksum = result.getChecksum();
        final int blockSize = result.getWrittenBlockSize();

        // write trailer
        final ByteBuffer trailer = ByteBuffer.allocate(Constant.kBlockTrailerSize);
        trailer.putLong(checksum);
        trailer.flip();
        fileChannel.write(trailer);

        pendingDataBlock.reset();
        nextBlockOffset += blockSize + Constant.kBlockTrailerSize;

        return new BlockIndexHandle(blockStartOffset, blockSize);
    }
}
