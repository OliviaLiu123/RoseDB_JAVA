package main.java.wal.segment;


/*
StartupBlock 用于启动遍历期间缓存读取的块数据，
由于启动遍历期间只有一个读取器（单线程）因此可以使用一个块来完成整个遍历，以免内存分配
 */
public class StartupBlock {
    private byte[] block; // 用于缓存裤子数据的字节数组
    private long blockNumber;// 块编号

    //construct
    public StartupBlock() {
        this.block = new byte[SegmentConstants.BLOCK_SIZE];
        this.blockNumber = -1;
    }

    //getter and setter

    public byte[] getBlock() {
        return block;
    }

    public void setBlock(byte[] block) {
        this.block = block;
    }

    public long getBlockNumber() {
        return blockNumber;
    }

    public void setBlockNumber(long blockNumber) {
        this.blockNumber = blockNumber;
    }
}
