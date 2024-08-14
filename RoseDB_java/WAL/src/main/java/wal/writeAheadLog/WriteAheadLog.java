package main.java.wal.writeAheadLog;

import main.java.wal.options.Options;
import main.java.wal.segment.Segment;
import main.java.wal.segment.ChunkPosition;
import main.java.wal.segment.SegmentConstants;
import main.java.wal.segment.SegmentReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// 这个类用于提供数据写入的持久性和容错性
public class WriteAheadLog {
    private Segment activeSegment;//当前活跃段文件，用于新数据的写入
    private Map<Integer,Segment> olderSegments;// 存储旧的段文件，只用于读取操作

    private Options options;// WAl 的配置选项
    private ReentrantReadWriteLock mu;// 用于并发访问WAL 数据结构

    private int bytesWrite; // 跟踪写入的字节数

    private List<Integer> renameIds; // 存储需要重命名的段文件ID

    private List<byte[]> pendingWrites;// 存储待写入的数据
    private long pendingSize;// 待写入数据的总大小

    private ReentrantLock pendingWritesLock;// 用于同步 pendingWrites 的访问

    private static final int INITIAL_SEGMENT_FILE_ID = 1;


    // 构造函数
    public WriteAheadLog(Options options) {
        // 初始化传入的选项
        this.options = options != null ? options : new Options();

        // 初始化数据结构
        this.olderSegments = new HashMap<>();
        this.mu = new ReentrantReadWriteLock();
        this.renameIds = new ArrayList<>();
        this.pendingWrites = new ArrayList<>();
        this.pendingSize = 0;
        this.bytesWrite = 0;

        // 初始化锁
        this.pendingWritesLock = new ReentrantLock();

        // 其他初始化逻辑
        this.activeSegment = null; // 活跃段文件需要在其他地方初始化或通过方法设置
    }

//1.打开WAL 并返回实例
    public static WriteAheadLog open(Options options) throws IOException{
        WriteAheadLog wal = new WriteAheadLog(options);
        Files.createDirectories(Paths.get(options.getDirPath()));// 确保指定的目录存在，如果目录不存在则创建

        List<Integer> segmentIDs = new ArrayList<>();
        DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(options.getDirPath()));
//使用 Files.newDirectoryStream 方法打开一个目录流，以便遍历目录中的所有文件。
        for(Path entry : stream){
            String fileName = entry.getFileName().toString();
            if(fileName.endsWith(options.getSegmentFileExt())){
                int id = Integer.parseInt(fileName.replace(options.getSegmentFileExt(),""));
                segmentIDs.add(id);
            }
        }
        // 处理段文件
        if(segmentIDs.isEmpty()){
            wal.activeSegment = Segment.openSegmentFile(options.getDirPath(),options.getSegmentFileExt(),INITIAL_SEGMENT_FILE_ID);
        }else{
            Collections.sort(segmentIDs);
            for(int i = 0; i <segmentIDs.size();i++){
                int segId = segmentIDs.get(i);
                Segment segment = Segment.openSegmentFile(options.getDirPath(),options.getSegmentFileExt(),segId);
                if(i==segmentIDs.size()-1){
                    wal.activeSegment = segment;
                }else{
                    wal.olderSegments.put(segId,segment);//如果是最后一个段文件，则将其设为当前活动的段文件 activeSegment。
                    //否则，将其存储在 olderSegments 中，表示它是一个旧的段文件，只用于读取。
                }
            }
        }
return wal;

    }


// 2.返回当前活跃段id
public int getActiveSegmentId(){
        mu.readLock().lock(); // 读取锁 确保并发安全
        try{
            return activeSegment.getSegmentId();
        }finally{
            mu.readLock().unlock();
        }
}
//3.检查WAL 是否为空
    public boolean isEmpty(){
        mu.readLock().lock();
        try{
            return olderSegments.isEmpty()&&activeSegment.size()==0;
        }finally{
            mu.readLock().unlock();
        }
    }


//4.设置是否在启动时遍历段文件
public void setIsStartupTraversal(boolean v) {
    mu.writeLock().lock();
    try {
        for (Segment seg : olderSegments.values()) {
            seg.setStartupTraversal(v);
        }
        activeSegment.setStartupTraversal(v);
    } finally {
        mu.writeLock().unlock();
    }
}

//5.创建一个新的活跃段文件
    // 修改return boolean
// 创建一个新的活跃段文件
public boolean openNewActiveSegment() {
    mu.writeLock().lock();
    try {
        if (this.activeSegment != null) {
            // 尝试同步当前活跃段
            activeSegment.sync();
        }

        // 确定新段的 ID，如果 activeSegment 为 null 则使用初始段 ID
        int newSegmentId = (this.activeSegment != null) ? activeSegment.getSegmentId() + 1 : INITIAL_SEGMENT_FILE_ID;

        // 打开一个新的段文件，并将其设为活跃段
        Segment segment = Segment.openSegmentFile(options.getDirPath(), options.getSegmentFileExt(), newSegmentId);

        // 如果之前有活跃段，将旧的活跃段移动到旧段集合中
        if (this.activeSegment != null) {
            olderSegments.put(activeSegment.getSegmentId(), activeSegment);
        }

        // 更新活跃段
        activeSegment = segment;

        System.out.println("New active segment initialized with ID: " + newSegmentId);

        return true; // 成功返回 true
    } catch (IOException e) {
        e.printStackTrace(); // 记录异常信息
        return false; // 失败返回 false
    } finally {
        mu.writeLock().unlock();
    }
}





    //6. 清空pendingWrites 并重置 pendingSize
    public void clearPendingWrites(){
        pendingWritesLock.lock();
        try{
            pendingSize = 0;
            pendingWrites.clear();
        }finally {
            pendingWritesLock.unlock();
        }
    }

//7. 添加数据到pendingWrites 并等待批量写入
    public void pendingWrites(byte[] data){
        pendingWritesLock.lock();
        try{
            long size = maxDataWriteSize(data.length);
            pendingSize +=size;
            pendingWrites.add(data);
        }finally{
            pendingWritesLock.unlock();
        }
    }

    // 8.计算可能的最大写入大小
    private long maxDataWriteSize(long size) {
        return SegmentConstants.CHUNCK_HEADER_SIZE + size + ((size / SegmentConstants.BLOCK_SIZE + 1) * SegmentConstants.CHUNCK_HEADER_SIZE);
    }

    // 9.检查当前段文件是否已满
    private boolean isFull(long delta) {
        return activeSegment.size() + maxDataWriteSize(delta) > options.getSegmentSize();
    }

    // 10.批量写入 pendingWrites 数据到 WAL，并清空 pendingWrites
    public List<ChunkPosition> writeAll() throws IOException {
        if (pendingWrites.isEmpty()) {
            System.out.println("writeAll: No pending writes to process.");
            return new ArrayList<>();
        }
        mu.writeLock().lock();
        try {

            if (pendingSize > options.getSegmentSize()) {
                System.err.println("writeAll: Pending size too large: " + pendingSize + " bytes.");
                throw new IOException("Pending size too large");
            }

            if (activeSegment.size() + pendingSize > options.getSegmentSize()) {
                System.out.println("writeAll: Rotating active segment.");
                rotateActiveSegment();
            }

            // 新的 writeAll 逻辑，逐条写入 pendingWrites
            List<ChunkPosition> chunkPositions = new ArrayList<>();

            for (byte[] data : pendingWrites) {
                if (data != null) {
                    ChunkPosition position = activeSegment.write(data);  // 假设 write 是将数据写入段文件的方法
                    if (position != null) {
                        chunkPositions.add(position);
                    }
                }
            }
            clearPendingWrites();

            return chunkPositions;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            mu.writeLock().unlock();
        }
    }
    private static final Logger logger = LoggerFactory.getLogger(WriteAheadLog.class);
    // 11.将数据写入 WAL，并返回数据的位置
    public ChunkPosition write(byte[] data) throws Exception {
        mu.writeLock().lock();
        try {
            if (data.length + SegmentConstants.CHUNCK_HEADER_SIZE > options.getSegmentSize()) {
                throw new IOException("Data too large");
            }

            if (isFull(data.length)) {
                rotateActiveSegment();
            }


            logger.info("Writing data to WAL: " + Arrays.toString(data));
            logger.info("Current segment offset before write: " + activeSegment.getOffset());
            ChunkPosition position = activeSegment.write(data);
            logger.info("Data written. New ChunkPosition: " + position);
            logger.info("Current segment offset after write: " +position.getChunkOffset());

            bytesWrite += position.getChunkSize();

            boolean needSync = options.isSync() || (options.getBytesPerSync() > 0 && bytesWrite >= options.getBytesPerSync());
            if (needSync) {
                activeSegment.sync();
                bytesWrite = 0;
            }

            return position;
        } finally {
            mu.writeLock().unlock();
        }
    }

    // 12 读取 WAL 中的数据
    public byte[] read(ChunkPosition pos) throws Exception {
        mu.readLock().lock();
        try {

            Segment segment = (pos.getSegmentId() == activeSegment.getSegmentId()) ? activeSegment : olderSegments.get(pos.getSegmentId());
            if (segment == null) {
                System.out.println("Segment not found for Segment ID: " + pos.getSegmentId());
                throw new IOException("Segment not found");
            }
            return segment.read(pos.getBlockNumber(), pos.getChunkOffset());
        } finally {
            mu.readLock().unlock();
        }
    }

    // 13 关闭 WAL
    public void close() throws IOException {
        mu.writeLock().lock();
        try {
            for (Segment segment : olderSegments.values()) {
                segment.close();
                renameIds.add(segment.getSegmentId());
            }
            olderSegments.clear();

            renameIds.add(activeSegment.getSegmentId());
            activeSegment.close();
        } finally {
            mu.writeLock().unlock();
        }
    }

    // 14 删除 WAL 中的所有段文件
    public void delete() throws IOException {
        mu.writeLock().lock();
        try {
            for (Segment segment : olderSegments.values()) {
                segment.remove();
            }
            olderSegments.clear();

            activeSegment.remove();
        } finally {
            mu.writeLock().unlock();
        }
    }

    // 15 同步活跃段文件到磁盘
    public void sync() throws IOException {
        mu.writeLock().lock();
        try {
            activeSegment.sync();
        } finally {
            mu.writeLock().unlock();
        }
    }

    // 16 重命名所有段文件的扩展名
    public void renameFileExt(String ext) throws IOException {
        if (!ext.startsWith(".")) {
            throw new IllegalArgumentException("Segment file extension must start with '.'");
        }

        mu.writeLock().lock();
        try {
            for (int id : renameIds) {
                String oldName = segmentFileName(options.getDirPath(), options.getSegmentFileExt(), id);
                String newName = segmentFileName(options.getDirPath(), ext, id);
                Files.move(Paths.get(oldName), Paths.get(newName));
            }

            options.setSegmentFileExt(ext);
        } finally {
            mu.writeLock().unlock();
        }
    }

    // 17 辅助方法：计算段文件名
    public static String segmentFileName(String dirPath, String extName, int id) {
        System.out.println("segmentFileName id is :"+id);
        return Paths.get(dirPath, String.format("%09d" + extName, id)).toString();
    }

    // 18 辅助方法：创建新的段文件并替换当前活跃段文件
    private void rotateActiveSegment() throws IOException {
        activeSegment.sync();
        bytesWrite = 0;
        Segment segment = Segment.openSegmentFile(options.getDirPath(), options.getSegmentFileExt(), activeSegment.getSegmentId() + 1);
        olderSegments.put(activeSegment.getSegmentId(), activeSegment);
        activeSegment = segment;
    }

    // 19 创建一个只读取 ID 小于或等于给定 ID 的 Reader
    public Reader newReaderWithMax(int segId) {
        mu.readLock().lock();
        try {
            List<SegmentReader> segmentReaders = new ArrayList<>();
            for (Segment segment : olderSegments.values()) {
                if (segId == 0 || segment.getSegmentId() <= segId) {
                    segmentReaders.add(segment.newReader());
                }
            }
            if (segId == 0 || activeSegment.getSegmentId() <= segId) {
                segmentReaders.add(activeSegment.newReader());
            }

            segmentReaders.sort((s1, s2) -> Integer.compare(s1.getSegment().getSegmentId(), s2.getSegment().getSegmentId()));

            return new Reader(segmentReaders);
        } finally {
            mu.readLock().unlock();
        }
    }

    // 20 创建一个从给定位置开始读取的 Reader
    public Reader newReaderWithStart(ChunkPosition startPos) throws IOException {
        if (startPos == null) {
            throw new IllegalArgumentException("start position is null");
        }

        mu.readLock().lock();
        try {
            Reader reader = newReader();
            while (true) {
                if (reader.getCurrentSegmentId() < startPos.getSegmentId()) {
                    reader.skipCurrentSegment();
                    continue;
                }

                ChunkPosition currentPos = reader.currentChunkPosition();
                if (currentPos.getBlockNumber() >= startPos.getBlockNumber() && currentPos.getChunkOffset() >= startPos.getChunkOffset()) {
                    break;
                }

                reader.next(); // 寻找匹配位置
            }
            return reader;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            mu.readLock().unlock();
        }
    }

    // 21 创建一个读取所有段文件数据的 Reader
    public Reader newReader() {
        return newReaderWithMax(0);
    }





//getter and setter


    public Segment getActiveSegment() {
        return activeSegment;
    }

    public void setActiveSegment(Segment activeSegment) {
        this.activeSegment = activeSegment;
    }

    public Map<Integer, Segment> getOlderSegments() {
        return olderSegments;
    }

    public void setOlderSegments(Map<Integer, Segment> olderSegments) {
        this.olderSegments = olderSegments;
    }

    public Options getOptions() {
        return options;
    }

    public void setOptions(Options options) {
        this.options = options;
    }

    public ReentrantReadWriteLock getMu() {
        return mu;
    }

    public void setMu(ReentrantReadWriteLock mu) {
        this.mu = mu;
    }

    public int getBytesWrite() {
        return bytesWrite;
    }

    public void setBytesWrite(int bytesWrite) {
        this.bytesWrite = bytesWrite;
    }

    public List<Integer> getRenameIds() {
        return renameIds;
    }

    public void setRenameIds(List<Integer> renameIds) {
        this.renameIds = renameIds;
    }

    public List<byte[]> getPendingWrites() {
        return pendingWrites;
    }

    public void setPendingWrites(List<byte[]> pendingWrites) {
        this.pendingWrites = pendingWrites;
    }

    public long getPendingSize() {
        return pendingSize;
    }

    public void setPendingSize(long pendingSize) {
        this.pendingSize = pendingSize;
    }

    public ReentrantLock getPendingWritesLock() {
        return pendingWritesLock;
    }

    public void setPendingWritesLock(ReentrantLock pendingWritesLock) {
        this.pendingWritesLock = pendingWritesLock;
    }
}
