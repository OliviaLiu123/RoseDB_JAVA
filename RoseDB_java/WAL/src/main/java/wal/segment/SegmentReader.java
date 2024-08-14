package main.java.wal.segment;

import java.io.IOException;

/**
 * SegmentReader 类用于从段文件中按顺序遍历和读取数据块。
 * 你可以调用 next 方法来获取下一个数据块，当没有数据时，会返回 EOF。
 */
public class SegmentReader {
    private Segment segment;  // 当前关联的段文件
    private int blockNumber;  // 当前读取的块编号
    private long chunkOffset;  // 当前块内的数据偏移量

    public SegmentReader(Segment segment) {
        this.segment = segment;
        this.blockNumber = 0;  // 初始化块编号为 0
        this.chunkOffset = 0;  // 初始化块内偏移量为 0
    }

    public main.java.wal.segment.ChunkPosition getCurrentChunkPosition() {
        return new main.java.wal.segment.ChunkPosition(segment.getSegmentId(), blockNumber, chunkOffset, 0);
    }

    public SegmentReader(Segment segment, int blockNumber, long chunkOffset) {
        this.segment = segment;
        this.blockNumber = blockNumber;
        this.chunkOffset = chunkOffset;
    }

    public Segment getSegment() {
        return segment;
    }

    public void setSegment(Segment segment) {
        this.segment = segment;
    }

    public int getBlockNumber() {
        return blockNumber;
    }

    public void setBlockNumber(int blockNumber) {
        this.blockNumber = blockNumber;
    }

    public long getChunkOffset() {
        return chunkOffset;
    }

    public void setChunkOffset(long chunkOffset) {
        this.chunkOffset = chunkOffset;
    }

    // 返回下一个数据块
    public byte[] next() throws Exception{
        if(segment.isClosed())throw Errors.ERR_CLOSED;
        ChunkPosition chunkPosition = new ChunkPosition(segment.getSegmentId(),blockNumber,chunkOffset,0);
        byte[] value = segment.readInternal(blockNumber,chunkOffset).getKey();
        // 更新块的偏移 和块编码
        this.blockNumber = chunkPosition.getBlockNumber();
        this.chunkOffset = chunkPosition.getChunkOffset();
        return value;

    }


}
