package main.java.wal.segment;

import java.nio.ByteBuffer;

/**
 * ChunkPosition 类用于表示段文件中的某个数据块的位置。
 */
public class ChunkPosition {
    private int segmentId;// 段文件的 ID
    private int blockNumber;// 块编号

    private long chunkOffset;// 数据块在段文件中的起始偏移量
    private int chunkSize;// 数据块的大小，以字节为单位

    public ChunkPosition(int segmentId, int blockNumber, long chunkOffset, int chunkSize) {
        this.segmentId = segmentId;
        this.blockNumber = blockNumber;
        this.chunkOffset = chunkOffset;
        this.chunkSize = chunkSize;
    }

//将ChunkPosition 编码为字节数组
    public byte[] encode(){
        return encode(true);
    }
// 将 ChunkPosition 编码为固定大小的字节数组
    public byte[] encodeFixedSize(){
        return encode(false);
    }
// 内部方法，执行实际编码操作
    private byte[] encode(boolean shrink){
        ByteBuffer buffer = ByteBuffer.allocate(SegmentConstants.MAX_LEN); // 表示缓冲区的最大长度

        // 编码 SegmentId
        buffer.putInt(segmentId);

        // 编码 BlockNumber
        buffer.putInt(blockNumber);

        // 编码 ChunkOffset
        buffer.putLong(chunkOffset);

        // 编码 ChunkSize
        buffer.putInt(chunkSize);

        if (shrink) {
            byte[] result = new byte[buffer.position()];
            buffer.flip();
            buffer.get(result);
            return result;
        } else {
            return buffer.array();
        }    }


    // 静态方法，解码字节数组，生成 ChunkPosition 对象
    public static ChunkPosition decodeChunkPosition(byte[] buf) {
        if (buf == null || buf.length == 0) {
            return null;
        }

        ByteBuffer buffer = ByteBuffer.wrap(buf);

        // 解码 SegmentId
        int segmentId = buffer.getInt();

        // 解码 BlockNumber
        int blockNumber = buffer.getInt();

        // 解码 ChunkOffset
        long chunkOffset = buffer.getLong();

        // 解码 ChunkSize
        int chunkSize = buffer.getInt();

        return new ChunkPosition(segmentId, blockNumber, chunkOffset, chunkSize);
    }

    //getter and setter

    public int getSegmentId() {
        return segmentId;
    }

    public void setSegmentId(int segmentId) {
        this.segmentId = segmentId;
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

    public int getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }
}
