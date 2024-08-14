package main.java.wal.segment;

// 段文件管理。 主要包括对端文件的操作（写入，读取同步）。以及对段文件内部数据块的处理（如计算位置，和效验）


import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.ImmutablePair;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32;

//Segment 表示WAL 中的一个段文件
// 段文件是追加写入的，数据按块写入，每个块的大小是32kb
public class Segment {
    private static int segmentId;// 段文件的唯一标识符
    private RandomAccessFile file;// 文件描述符，用于读写段文件
    private int currentBlockNumber;// 当前写入的块编号

    private int currentBlockSize;// 当前块已写入的数据大小
    private boolean closed;// 指示段文件是否已关闭
    private byte[] header;// 块头信息缓冲区
    private StartupBlock startupBlock;// 启动时用于读取的块数据结构

    private boolean isStartupTraversal;// 是否在启动时遍历段文件


    //构造函数


    public Segment(int segmentId, RandomAccessFile file, int currentBlockNumber, int currentBlockSize) {
        this.segmentId = segmentId;
        this.file = file;
        this.currentBlockNumber = currentBlockNumber;
        this.currentBlockSize = currentBlockSize;
        this.closed = false;
        this.header = new byte[SegmentConstants.CHUNCK_HEADER_SIZE];
        this.startupBlock = new StartupBlock();
        this.isStartupTraversal = false;
    }
    //打开新的段文件
    public static Segment openSegmentFile(String dirPath, String extName, int id) throws IOException{

        File file = new File(dirPath,String.format("%09d"+extName,id));
        RandomAccessFile fd = new RandomAccessFile(file,"rw");

        long offset = fd.length();
        int currentBlockNumber = (int) (offset/SegmentConstants.BLOCK_SIZE);
        int currentBlockSize = (int)(offset%SegmentConstants.BLOCK_SIZE);
        return new Segment(id,fd,currentBlockNumber,currentBlockSize);
    }
    //创建一个SegmentReader 用于遍历段文件中的数据块
    public SegmentReader newReader(){
        return new SegmentReader(this,0,0);
    }
    // 同步段文件到磁盘
    public void sync() throws IOException{
        if(closed){
            return;
        }
        file.getFD().sync();
    }
//关闭段文件
    public void close() throws IOException{
        if(closed){
            return;
        }
        closed = true;
        file.close();
    }

//删除段文件
    public void remove() throws IOException{
        close();
        new File(file.getFD().toString()).delete();
    }

//返回段文件大小
    public long size(){
        return (long) currentBlockNumber*SegmentConstants.BLOCK_SIZE+currentBlockSize;
    }

//向缓冲区写入数据
    public ChunkPosition writeToBuffer(byte[] data, ByteBuffer chunkBuffer) throws Exception{
        int startBufferLen = chunkBuffer.position();
        int padding =0;

        if(closed) throw Errors.ERR_CLOSED;

        //如果剩余块大小不足以容纳块头，则填充块
        if(currentBlockSize+SegmentConstants.CHUNCK_HEADER_SIZE >= SegmentConstants.BLOCK_SIZE){
            if(currentBlockSize <SegmentConstants.CHUNCK_HEADER_SIZE){
                padding = SegmentConstants.BLOCK_SIZE-currentBlockSize;
                chunkBuffer.put(new byte[padding]);

                currentBlockNumber++;
                currentBlockSize = 0;
            }

        }
        //返回数据块的起始位置，用户可以使用它来读取数据

        ChunkPosition position = new ChunkPosition(segmentId,currentBlockNumber,currentBlockSize,0);
        int dataSize= data.length;
        if(currentBlockSize+dataSize+SegmentConstants.CHUNCK_HEADER_SIZE <= SegmentConstants.BLOCK_SIZE){
            appendChunkBuffer(chunkBuffer,data,ChunkType.CHUNK_TYPE_FULL);
            position.setChunkSize(dataSize+SegmentConstants.CHUNCK_HEADER_SIZE);
        }else{
            int leftSize = dataSize;
            int blockCount = 0;
            while(leftSize >0){
                int chunkSize = SegmentConstants.BLOCK_SIZE -currentBlockSize-SegmentConstants.CHUNCK_HEADER_SIZE;
                if(chunkSize >leftSize){
                    chunkSize = leftSize;
                }
                appendChunkBuffer(chunkBuffer, data, determineChunkType(leftSize, chunkSize, dataSize));

                leftSize -= chunkSize;
                blockCount++;
                currentBlockSize = (currentBlockSize +chunkSize +SegmentConstants.CHUNCK_HEADER_SIZE) % SegmentConstants.BLOCK_SIZE;
            }
            position .setChunkSize(blockCount*SegmentConstants.CHUNCK_HEADER_SIZE+dataSize);
            }

        int endBufferLen = chunkBuffer.position();
        if(position.getChunkSize()+padding != endBufferLen -startBufferLen){
            throw new Exception(String.format("chunk size mismatch! Excepted: %d, Actual: %d",position.getChunkSize() + padding, endBufferLen - startBufferLen));

        }
        currentBlockSize += position.getChunkSize();
        if(currentBlockSize >=SegmentConstants.BLOCK_SIZE){
            currentBlockNumber +=currentBlockSize/SegmentConstants.BLOCK_SIZE;
            currentBlockSize%=SegmentConstants.BLOCK_SIZE;
        }

        return position;
        }



    // 确定块类型（FULL、FIRST、MIDDLE、LAST）
    private ChunkType determineChunkType(int leftSize, int chunkSize, int dataSize){
        if(leftSize == dataSize) return ChunkType.CHUNK_TYPE_FIRST;
        if(leftSize ==chunkSize) return ChunkType.CHUNK_TYPE_LAST;
        return ChunkType.CHUNK_TYPE_MIDDLE;
    }
//  向缓冲区附加数据块
    private void appendChunkBuffer(ByteBuffer buf,byte[] data,ChunkType chunkType){
        CRC32 crc32 = new CRC32();
        //设置块头
        buf.putShort(4,(short)data.length);
        buf.put(6,(byte) chunkType.ordinal());
        crc32.update(buf.array(),4,buf.array().length-4);
        crc32.update(data);
        buf.putInt(0,(int) crc32.getValue());

        buf.put(header);
        buf.put(data);
    }

// 批量写入数据到段文件
    public List<ChunkPosition> writeAll(List<byte[]> data) throws Exception{
        if(closed) throw Errors.ERR_CLOSED;

        int originBlockNumber = currentBlockNumber;
        int originBlockSize = currentBlockSize;

        ByteBuffer chunkBuffer = ByteBuffer.allocate(SegmentConstants.BLOCK_SIZE);
        List<ChunkPosition> positions = new ArrayList<>();

        for(byte[] d: data){
            ChunkPosition pos = writeToBuffer(d,chunkBuffer);
            if(pos !=null){
                positions.add(pos);
            }

        }
        try{
            writeChunkBuffer(chunkBuffer);
        }catch (Exception e){
            currentBlockNumber = originBlockNumber;
            currentBlockSize = originBlockSize;
            throw e;
        }

        return positions;
    }

// 将单个数据写入段文件
    public ChunkPosition write(byte[] data) throws Exception{
        if (closed) {
            System.err.println("write: Operation attempted on a closed segment.");
            throw Errors.ERR_CLOSED;
        }
        int originBlockNumber = currentBlockNumber;
        int originBlockSize = currentBlockSize;
        ByteBuffer chunkBuffer = ByteBuffer.allocate(SegmentConstants.BLOCK_SIZE);
        ChunkPosition pos = writeToBuffer(data,chunkBuffer);
        try{
            writeChunkBuffer(chunkBuffer);
            System.out.println("write: Chunk buffer written successfully.");
        }catch (Exception e) {
            currentBlockNumber = originBlockNumber;
            currentBlockSize = originBlockSize;
            System.err.println("write: Exception occurred, restoring original block number and size.");
            throw e;
        }
        // 返回写入的位置
        System.out.println("write: Write operation completed, returning position: " + pos);
        return pos;
    }



// 将缓冲区中的数据写入段文件
    private void writeChunkBuffer(ByteBuffer buf) throws IOException{
        if(currentBlockSize > SegmentConstants.BLOCK_SIZE){
            throw new IOException("The current block size exceeds the maximum block size");
        }

        // 打印写入文件之前的缓冲区内容
        file.write(buf.array());
        startupBlock.setBlockNumber(-1);
        System.out.println("writeChunkBuffer: Buffer written successfully, startup block set to -1.");
    }
    // 根据块号和偏移读取段文件中的数据
    public byte[] read(int blockNumber, long chunkOffset) throws Exception {

        return readInternal(blockNumber, chunkOffset).getLeft();
    }


    // 帮助读取数据块
// 内部方法，帮助读取数据块
// 根据块号和偏移读取段文件中的数据


    // 内部方法，帮助读取数据块
    Pair<byte[], ChunkPosition> readInternal(int blockNumber, long chunkOffset) throws Exception {
        if (closed) {
            System.out.println("Error: Segment is closed.");
            throw Errors.ERR_CLOSED;
        }

        byte[] result = null;
        byte[] block = new byte[SegmentConstants.BLOCK_SIZE];
        long segSize = size();
        ChunkPosition nextChunk = new ChunkPosition(segmentId, blockNumber, chunkOffset, 0);


        while (true) {
            long size = SegmentConstants.BLOCK_SIZE;
            long offset = (long) blockNumber * SegmentConstants.BLOCK_SIZE;
            if (size + offset > segSize) {
                size = segSize - offset;
            }


            if (chunkOffset >= size) {
                System.out.println("Chunk offset exceeds block size. Returning null.");
                return new ImmutablePair<>(null, null);
            }

            // 读取文件数据到 block
            file.seek(offset);
            file.read(block, 0, (int) size);
            System.out.println("Block read successfully.");

            // 提取块头信息
            byte[] header = new byte[SegmentConstants.CHUNCK_HEADER_SIZE];
            System.arraycopy(block, (int) chunkOffset, header, 0, SegmentConstants.CHUNCK_HEADER_SIZE);
            System.out.println("Header extracted successfully.");
            System.out.println("Header bytes: " + Arrays.toString(header));


            // 解析数据长度
            int length = ByteBuffer.wrap(header, 4, 2).getShort();
            System.out.println("Data length extracted from header: " + length);

            int start = (int) chunkOffset + SegmentConstants.CHUNCK_HEADER_SIZE;
            byte[] data = new byte[length];
            System.arraycopy(block, start, data, 0, length);
            System.out.println("Data copied from block to result array.");

            // 计算并验证 CRC 校验码
            CRC32 crc32 = new CRC32();
            crc32.update(block, (int) chunkOffset + 4, length);
            long checksum = crc32.getValue();

            if (checksum != ByteBuffer.wrap(header, 0, 4).getInt()) {
                System.out.println("Invalid CRC. Expected: " + ByteBuffer.wrap(header, 0, 4).getInt() + ", Found: " + checksum);
                throw new IOException("Invalid CRC");
            }

            int chunkType = header[6];
            System.out.println("Chunk type: " + chunkType);

            if (chunkType == ChunkType.CHUNK_TYPE_FULL.ordinal() || chunkType == ChunkType.CHUNK_TYPE_LAST.ordinal()) {
                nextChunk.setBlockNumber(blockNumber);
                nextChunk.setChunkOffset(chunkOffset + SegmentConstants.CHUNCK_HEADER_SIZE + length);

                if (nextChunk.getChunkOffset() + SegmentConstants.CHUNCK_HEADER_SIZE >= SegmentConstants.BLOCK_SIZE) {
                    nextChunk.setBlockNumber(blockNumber + 1);
                    nextChunk.setChunkOffset(0);
                    System.out.println("Moving to the next block: Block Number = " + nextChunk.getBlockNumber());
                }
                break;
            }
            blockNumber++;
            chunkOffset = 0;
        }

        System.out.println("Data read successfully, returning result.");
        return new ImmutablePair<>(result, nextChunk);
    }
    public int getOffset() {
        // 返回当前块的起始位置
        return currentBlockNumber * SegmentConstants.BLOCK_SIZE + currentBlockSize;
    }

    //getter and setter
    public static int getSegmentId() {
        return segmentId;
    }

    public void setSegmentId(int segmentId) {
        this.segmentId = segmentId;
    }

    public RandomAccessFile getFile() {
        return file;
    }

    public void setFile(RandomAccessFile file) {
        this.file = file;
    }

    public int getCurrentBlockNumber() {
        return currentBlockNumber;
    }

    public void setCurrentBlockNumber(int currentBlockNumber) {
        this.currentBlockNumber = currentBlockNumber;
    }

    public int getCurrentBlockSize() {
        return currentBlockSize;
    }

    public void setCurrentBlockSize(int currentBlockSize) {
        this.currentBlockSize = currentBlockSize;
    }

    public boolean isClosed() {
        return closed;
    }

    public void setClosed(boolean closed) {
        this.closed = closed;
    }

    public byte[] getHeader() {
        return header;
    }

    public void setHeader(byte[] header) {
        this.header = header;
    }

    public StartupBlock getStartupBlock() {
        return startupBlock;
    }

    public void setStartupBlock(StartupBlock startupBlock) {
        this.startupBlock = startupBlock;
    }

    public boolean isStartupTraversal() {
        return isStartupTraversal;
    }

    public void setStartupTraversal(boolean startupTraversal) {
        isStartupTraversal = startupTraversal;
    }




}
