package main.java.com.rosedb.db;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import main.java.com.rosedb.options.BatchOptions;
import main.java.com.rosedb.record.IndexRecord;
import main.java.com.rosedb.record.LogRecord;
import main.java.com.rosedb.record.RecordConstants;
import main.java.com.rosedb.watch.Event;
import main.java.com.rosedb.watch.Watcher;
import main.java.com.rosedb.Batch;
import main.java.com.rosedb.index.Indexer;
import main.java.com.rosedb.options.Options;
import main.java.com.rosedb.Merge;
import org.apache.commons.lang3.tuple.Pair;
import main.java.wal.segment.ChunkPosition;
import main.java.wal.segment.SegmentReader;
import main.java.wal.writeAheadLog.Reader;
import main.java.wal.writeAheadLog.WriteAheadLog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import main.java.com.rosedb.index.*;

import static main.java.com.rosedb.Merge.getMergeFinSegmentId;

public class DB {
    // WAL日志文件，存储数据库的所有操作记录，用于数据恢复
    private WriteAheadLog dataFiles;

    // Hint文件，用于快速加载数据库索引
    private WriteAheadLog hintFile;

    // 索引器，用于在内存中维护键值对的索引
    private MemoryTreeMap index ;

    // 数据库的配置信息，如文件路径、同步策略等
    private main.java.com.rosedb.options.Options options;

    // 文件锁，防止多个进程同时访问同一个数据库目录
    private FileLock fileLock;

    // 读写锁，确保线程安全地访问数据库
    private ReentrantReadWriteLock mu ;

    // 数据库关闭状态标志，表示数据库是否已关闭
    private boolean closed ;

    // 数据合并状态标志，表示是否正在进行数据合并操作
    private boolean mergeRunning ;

    // 批处理对象池，用于重用Batch对象，减少对象创建的开销
    private SimpleObjectPool<Batch> batchPool;
    private SimpleObjectPool<LogRecord> recordPool;

    // 编码头信息，用于数据记录的编码过程
    private byte[] encodeHeader ;

    private static DB db = new DB(); // 需要用实际的DB初始化

    // 默认的 BatchOptions 实例，可以在对象池中使用
    private BatchOptions defaultBatchOptions ;



    private ReentrantReadWriteLock lock ;


    // 日志记录对象池，用于重用LogRecord对象，减少对象创建的开销

    // 事件通道，用于传递数据库事件给监听器
    private BlockingQueue<Event> watchCh ;

    // 事件监听器，用于处理数据库事件
    private Watcher watcher;

    // 标记删除过期键的游标位置，用于在长时间操作中保存进度
    private byte[] expiredCursorKey;

    // 定时任务调度器，用于执行自动数据合并任务等
    private ScheduledExecutorService cronScheduler;


    // 构造函数
    public DB() {
        this.index = new MemoryTreeMap();  // Initialize MemoryTreeMap (Indexer implementation)
        this.watcher = new Watcher(10000);  // Initialize Watcher
        this.mu = new ReentrantReadWriteLock();  // Initialize ReentrantReadWriteLock
        this.closed = false;  // Default closed state to false
        this.mergeRunning = false;  // Default mergeRunning state to false
        this.encodeHeader = new byte[RecordConstants.MAX_LOG_RECORD_HEADER_SIZE];  // Initialize encodeHeader with appropriate size
        this.defaultBatchOptions = new BatchOptions(false, false);  // Initialize BatchOptions with default parameters
        this.lock = new ReentrantReadWriteLock();  // Initialize ReentrantReadWriteLock
        this.watchCh = new LinkedBlockingQueue<>();  // Initialize BlockingQueue for Event handling
        this.expiredCursorKey = new byte[0];  // Initialize expiredCursorKey with an empty array
        this.cronScheduler = Executors.newScheduledThreadPool(1);  // Initialize ScheduledExecutorService with a single thread

        // Initialize SimpleObjectPool for Batch
        this.batchPool = new SimpleObjectPool<>(10, new SimpleObjectPool.ObjectFactory<Batch>() {
            @Override
            public Batch createObject() {
                return new Batch(DB.this);  // Pass the DB instance to Batch constructor
            }
        });

        // Initialize SimpleObjectPool for LogRecord
        this.recordPool = new SimpleObjectPool<>(10, new SimpleObjectPool.ObjectFactory<LogRecord>() {
            @Override
            public LogRecord createObject() {
                return new LogRecord(new byte[0], new byte[0], (byte) 0, 0L, 0L);  // Create default LogRecord objects
            }
        });
    }



    // 启动定时任务
    public void startScheduledTask(Runnable task, long initialDelay, long period, TimeUnit unit) {
        cronScheduler.scheduleAtFixedRate(task, initialDelay, period, unit);
    }

    //  停止定时任务调度器
    public void stopScheduler() {
        cronScheduler.shutdown();
    }



    // 懒汉式实例化
    private static DB database;

    public static DB getInstance() throws IOException {
        if (database == null) {
            synchronized (DB.class) {
                if (database == null) {
                    database = new DB();
                }
            }
        }
        return database;
    }




    //1  打开数据库，使用指定的选项。如果数据库目录不存在，将自动创建。
// 如果另一个进程正在使用同一个数据库目录，将抛出异常。
    public static DB open(Options options) throws IOException {
        checkOptions(options); // 检查选项是否有效

        Path dirPath = Paths.get(options.getDirPath());
        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
        }

        DB dbInstance = getInstance();
        dbInstance.options = options;

        System.out.println("Opening WAL files...");
        dbInstance.dataFiles = dbInstance.openWalFiles();
        System.out.println("WAL files opened successfully.");

        System.out.println("Loading index...");
        if (!dbInstance.loadIndex()) {
            throw new IOException("Failed to load index");
        }
        System.out.println("Index loaded successfully.");

        System.out.println("Initializing batch pool...");
        dbInstance.initializeBatchPool();
        System.out.println("Batch pool initialized successfully.");

        return dbInstance;
    }

    private void initializeBatchPool() {
        this.batchPool = new SimpleObjectPool<>(10, new SimpleObjectPool.ObjectFactory<Batch>() {
            @Override
            public Batch createObject() {
                return new Batch(DB.this, new BatchOptions(false, false));
            }
        });
    }

    //2  打开WAL文件，用于写入数据和管理数据段
    public WriteAheadLog openWalFiles() throws IOException {
        return WriteAheadLog.open(new main.java.wal.options.Options(
                options.getDirPath(),        // 数据文件目录
                options.getSegmentSize(),    // 每个数据段的大小
                ".SEG",                      // 数据文件后缀
                options.isSync(),            // 是否同步
                options.getBytesPerSync()    // 每字节同步的数量
        ));
    }


    // 3 从WAL和Hint文件加载索引
    public boolean loadIndex() {
        System.out.println("call loadIndex....");
        try {
            Merge merge = new Merge(this);

            // 从 Hint 文件加载索引
            if (hintFile != null) {
                System.out.println("Loading index from hint file...");
                merge.loadIndexFromHintFile();
                System.out.println("Index loaded from hint file.");
            } else {
                System.out.println("Hint file is null. Skipping hint file loading.");
            }

            // 从 WAL 文件加载索引
            System.out.println("Loading index from WAL files...");
            loadIndexFromWAL();
            System.out.println("Index loaded from WAL files.");

            return true;
        } catch (Exception e) {
            System.out.println("Error occurred while loading index: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }




    // 4 关闭数据库并释放所有资源
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            if (closed) return;  // 如果已经关闭，则直接返回

            // 关闭WAL文件
            closeFiles();

            // 释放文件锁
            if (fileLock != null) {
                fileLock.release();
            }

            closed = true;  // 设置数据库已关闭标志
        } finally {
            lock.writeLock().unlock();  // 释放锁
        }
    }


    //5  关闭WAL文件和Hint文件
    public void closeFiles() throws IOException {
        if (dataFiles != null) {
            dataFiles.close();
        }
        if (hintFile != null) {
            hintFile.close();
        }
    }


    //6  将所有数据文件同步到底层存储
    public void sync() throws IOException {
        lock.writeLock().lock();
        try {
            dataFiles.sync();
        } finally {
            lock.writeLock().unlock();
        }
    }



    //7 返回数据库的统计信息，包括键的数量和数据库目录的磁盘大小
    public Stat stat() throws IOException {
        lock.readLock().lock();
        try {
            long diskSize = Files.size(Paths.get(options.getDirPath()));
            return new Stat(index.size(), diskSize);
        } finally {
            lock.readLock().unlock();
        }
    }


    //8 将键值对放入数据库，实际上是打开一个新的批处理并提交它
    public void put(byte[] key, byte[] value) throws Exception {
        Batch batch = batchPool.get();
        try {
            batch.init(false, false, this);

            // 在执行 put 操作前打印调试信息
            System.out.println("Putting data into batch: Key = " + new String(key) + ", Value = " + new String(value));

            batch.put(key, value);
            batch.commit();

            // 在成功提交后打印确认信息
            System.out.println("Successfully committed batch for Key = " + new String(key));
        } catch (Exception e) {
            System.err.println("Exception in put method: " + e.getMessage());
            e.printStackTrace();
        } finally {
            batch.reset();
            batchPool.put(batch);
        }
    }



    //9 将具有TTL（生存时间）的键值对放入数据库
    public void putWithTTL(byte[] key, byte[] value, Duration ttl) throws Exception {
        Batch batch = batchPool.get();
        try {
            batch.init(false, false, this);
            batch.putWithTTL(key, value, ttl);
            batch.commit();
        } finally {
            batch.reset();
            batchPool.put(batch);
        }
    }


    // 10 获取指定键的值
    public byte[] get(byte[] key) throws Exception {
        Batch batch = batchPool.get();
        try {
            batch.init(true, false, this);
            return batch.get(key);
        } finally {
            batch.commit();
            batch.reset();
            batchPool.put(batch);
        }
    }

    //11 删除指定的键
    public void delete(byte[] key) throws Exception {
        Batch batch = batchPool.get();
        try {
            batch.init(false, false, this);
            batch.delete(key);
            batch.commit();
        } finally {
            batch.reset();
            batchPool.put(batch);
        }
    }


    // 12 检查指定的键是否存在于数据库中
    public boolean exist(byte[] key) throws Exception {
        Batch batch = batchPool.get();
        try {
            batch.init(true, false, this);
            return batch.exist(key);
        } finally {
            batch.commit();
            batch.reset();
            batchPool.put(batch);
        }
    }

    // 13 设置键的TTL（生存时间）
    public void expire(byte[] key, Duration ttl) throws Exception {
        Batch batch = batchPool.get();
        try {
            batch.init(false, false, this);
            batch.expire(key, ttl);
            batch.commit();
        } finally {
            batch.reset();
            batchPool.put(batch);
        }
    }

    //14  获取键的TTL（生存时间）
    public Duration ttl(byte[] key) throws Exception {
        Batch batch = batchPool.get();
        try {
            batch.init(true, false, this);
            return batch.ttl(key);
        } finally {
            batch.commit();
            batch.reset();
            batchPool.put(batch);
        }
    }
    //15 移除键的TTL（生存时间），使其永久存在
    public void persist(byte[] key) throws Exception {
        Batch batch = batchPool.get();
        try {
            batch.init(false, false, this);
            batch.persist(key);
            batch.commit();
        } finally {
            batch.reset();
            batchPool.put(batch);
        }
    }

    //16 监视数据库中的事件，如果数据库没有启用Watch功能，则抛出异常
    public BlockingQueue<Event> watch() throws IOException {
        if (options.getWatchQueueSize() <= 0) {
            throw new IllegalStateException("Watch functionality is not enabled");
        }
        return watchCh;
    }

    //17  按升序遍历数据库中的所有键值对，并调用handleFn处理每个键值对
    public void ascend(BiConsumer<byte[], byte[]> handleFn) throws Exception {
        lock.readLock().lock();
        try {
            index.ascend((key, pos) -> {
                byte[] chunk = dataFiles.read(pos);
                byte[] value = checkValue(chunk);
                if (value != null) {
                    handleFn.accept(key, value);
                }
                return true;
            });
        } finally {
            lock.readLock().unlock();
        }
    }


    //18  按升序遍历指定范围内的键值对，并调用handleFn处理每个键值对
    public void ascendRange(byte[] startKey, byte[] endKey, BiConsumer<byte[], byte[]> handleFn) throws IOException {
        lock.readLock().lock();
        try {
            index.ascendRange(startKey, endKey, (key, pos) -> {
                byte[] chunk = dataFiles.read(pos);
                byte[] value = checkValue(chunk);
                if (value != null) {
                    handleFn.accept(key, value);
                }
                return true;
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.readLock().unlock();
        }
    }


    //19  按升序遍历大于或等于指定键的键值对，并调用handleFn处理每个键值对
    public void ascendGreaterOrEqual(byte[] key, BiConsumer<byte[], byte[]> handleFn) throws Exception {
        lock.readLock().lock();
        try {
            index.ascendGreaterOrEqual(key, (k, pos) -> {
                byte[] chunk = dataFiles.read(pos);
                byte[] value = checkValue(chunk);
                if (value != null) {
                    handleFn.accept(k, value);
                }
                return true;
            });
        } finally {
            lock.readLock().unlock();
        }
    }


    //20  按升序遍历所有键，并调用handleFn处理每个键
    public void ascendKeys(byte[] pattern, boolean filterExpired, Consumer<byte[]> handleFn) throws IOException {
        lock.readLock().lock();
        try {
            Pattern regexPattern = null;
            if (pattern != null && pattern.length > 0) {
                regexPattern = Pattern.compile(new String(pattern));
            }

            Pattern finalRegexPattern = regexPattern;
            index.ascend((key, pos) -> {
                if (finalRegexPattern != null && !finalRegexPattern.matcher(new String(key)).matches()) {
                    return true;
                }

                if (filterExpired) {
                    byte[] chunk = dataFiles.read(pos);
                    if (checkValue(chunk) == null) {
                        return true;
                    }
                }

                try {
                    handleFn.accept(key);
                } catch (Exception e) {
                    throw new IOException("Error processing key with Consumer", e);
                }
                return true;
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.readLock().unlock();
        }
    }


    // 21 按降序遍历数据库中的所有键值对，并调用handleFn处理每个键值对
    public void descend(BiConsumer<byte[], byte[]> handleFn) throws Exception {
        lock.readLock().lock();
        try {
            index.descend((key, pos) -> {
                byte[] chunk = dataFiles.read(pos);
                byte[] value = checkValue(chunk);
                if (value != null) {
                    handleFn.accept(key, value);
                }
                return true;
            });
        } finally {
            lock.readLock().unlock();
        }
    }


    //22  按降序遍历指定范围内的键值对，并调用handleFn处理每个键值对
    public void descendRange(byte[] startKey, byte[] endKey, BiConsumer<byte[], byte[]> handleFn) throws Exception {
        lock.readLock().lock();
        try {
            index.descendRange(startKey, endKey, (key, pos) -> {
                byte[] chunk = dataFiles.read(pos);
                byte[] value = checkValue(chunk);
                if (value != null) {
                    handleFn.accept(key, value);
                }
                return true;
            });
        } finally {
            lock.readLock().unlock();
        }
    }

    //23  按降序遍历小于或等于指定键的键值对，并调用handleFn处理每个键值对
    public void descendLessOrEqual(byte[] key, BiConsumer<byte[], byte[]> handleFn) throws IOException {
        lock.readLock().lock();
        try {
            index.descendLessOrEqual(key, (k, pos) -> {
                byte[] chunk = dataFiles.read(pos);
                byte[] value = checkValue(chunk);
                if (value != null) {
                    handleFn.accept(k, value);
                }
                return true;
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.readLock().unlock();
        }
    }

    //24  按降序遍历所有键，并调用handleFn处理每个键
    public void descendKeys(byte[] pattern, boolean filterExpired, Consumer<byte[]> handleFn) throws Exception {
        lock.readLock().lock();
        try {
            index.descend((key, pos) -> {
                if (filterExpired) {
                    byte[] chunk = dataFiles.read(pos);
                    if (checkValue(chunk) == null) {
                        return true;
                    }
                }
                handleFn.accept(key);
                return true;
            });
        } finally {
            lock.readLock().unlock();
        }
    }


    // 25 检查记录是否有效，如果是删除记录或已过期，则返回null
    private byte[] checkValue(byte[] chunk) {
        LogRecord record = LogRecord.decodeLogRecord(chunk);
        System.out.println("Chunk length in DB checkvalue: " + chunk.length);
        long now = System.nanoTime();
        if (record.getType() != RecordConstants.LOG_RECORD_DELETED && !record.isExpired(now)) {
            return record.getValue();
        }
        return null;
    }



    //26  检查数据库选项是否有效
    private static void checkOptions(Options options) {
        if (options.getDirPath() == null || options.getDirPath().isEmpty()) {
            throw new IllegalArgumentException("Path of database is empty");
        }
        if (options.getSegmentSize() <= 0) {
            throw new IllegalArgumentException("the size of database must be great than 0");
        }
        if (options.getAutoMergeCronExpr() != null && !options.getAutoMergeCronExpr().isEmpty()) {
            // 检查Cron表达式是否合法
            CronParser parser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX));
            parser.parse(options.getAutoMergeCronExpr());
        }
    }


    //27 从WAL文件加载索引，重建内存中的索引结构
    private void loadIndexFromWAL() throws IOException {
//        long mergeFinSegmentId = getMergeFinSegmentId(options.getDirPath());

//
//        Path mergeFinFilePath = Paths.get(options.getDirPath(), "000000001.MERGEFIN");
//        if (!Files.exists(mergeFinFilePath)) {
//            System.err.println("Merge finish file not found: " + mergeFinFilePath.toString());
//            throw new IOException("Merge finish file not found: " + mergeFinFilePath.toString());
//        }

        long mergeFinSegmentId = -1;
        try {
            mergeFinSegmentId = getMergeFinSegmentId(options.getDirPath());
        } catch (IOException e) {
            System.err.println("Merge finish file not found or could not be read: " + e.getMessage());
            // 如果找不到文件，视情况决定是否继续加载WAL
        }

        Map<Long, List<IndexRecord>> indexRecords = new HashMap<>();
        long now = System.nanoTime();

        Reader reader = dataFiles.newReader();
        dataFiles.setIsStartupTraversal(true);






        try {
            while (true) {
                System.out.println("Current Segment ID: " + reader.getCurrentSegmentId());
                if (reader.getCurrentSegmentId() <= mergeFinSegmentId) {
                    reader.skipCurrentSegment();
                    continue;
                }

                Pair<byte[], ChunkPosition> pair = reader.next();
                if (pair == null) {
                    System.out.println("No more chunks available, breaking out of loop.");
                    break;
                }
                byte[] chunk = pair.getLeft();
                if (chunk == null) {
                    throw new IllegalStateException("Chunk is null. Cannot proceed with decoding.");
                }else {
                    System.out.println("Chunk length: " + chunk.length);
                }

                LogRecord record = LogRecord.decodeLogRecord(chunk);

                SegmentReader currentReader = reader.getSegmentReaders().get(reader.getCurrentReaderIndex());
                int blockNumber = currentReader.getBlockNumber();
                long chunkOffset = currentReader.getChunkOffset();

                ChunkPosition position = new ChunkPosition(
                        reader.getCurrentSegmentId(),
                        blockNumber,
                        chunkOffset,
                        chunk.length
                );

                if (record.getType() == RecordConstants.LOG_RECORD_BATCH_FINISHED) {
                    long batchId = ByteBuffer.wrap(record.getKey()).getLong();

                    List<IndexRecord> batchRecords = indexRecords.get(batchId);
                    if (batchRecords != null) {
                        for (IndexRecord idxRecord : batchRecords) {
                            if (idxRecord.getRecordType() == RecordConstants.LOG_RECORD_NORMAL) {
                                index.put(idxRecord.getKey(), idxRecord.getPosition());
                            } else if (idxRecord.getRecordType() == RecordConstants.LOG_RECORD_DELETED) {
                                index.delete(idxRecord.getKey());
                            }
                        }
                        indexRecords.remove(batchId);
                    }
                } else if (record.getType() == RecordConstants.LOG_RECORD_NORMAL && record.getBatchId() == 0) {
                    index.put(record.getKey(), position);
                } else {
                    if (record.isExpired(now)) {
                        index.delete(record.getKey());
                        continue;
                    }
                    indexRecords.computeIfAbsent(record.getBatchId(), k -> new ArrayList<>())
                            .add(new IndexRecord(record.getKey(), record.getType(), position));
                }
            }
        } catch (IOException e) {
            // 处理IOException，可能包括EOF和其他文件读取错误
            System.err.println("Error occurred while loading index from WAL: " + e.getMessage());
            throw e;

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            dataFiles.setIsStartupTraversal(false);
        }
    }



    //28  删除所有过期的键，这个过程是耗时的，所以需要指定一个超时时间来防止数据库长时间不可用
    public void deleteExpiredKeys(Duration timeout) throws IOException {
        long now = System.nanoTime();

        lock.writeLock().lock();
        try {
            index.ascend((key, pos) -> {
                byte[] chunk = dataFiles.read(pos);
                LogRecord record = LogRecord.decodeLogRecord(chunk);
                if (record.isExpired(now)) {
                    index.delete(key);
                }
                return true;
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }














//getter and setter


    public WriteAheadLog getDataFiles() {
        return dataFiles;
    }

    public void setDataFiles(WriteAheadLog dataFiles) {
        this.dataFiles = dataFiles;
    }

    public WriteAheadLog getHintFile() {
        return hintFile;
    }

    public void setHintFile(WriteAheadLog hintFile) {
        this.hintFile = hintFile;
    }

    public Indexer getIndex() {
        return index;
    }

    public void setIndex(Indexer index) {
        this.index = (MemoryTreeMap) index;
    }

    public Options getOptions() {
        return options;
    }

    public void setOptions(Options options) {
        this.options = options;
    }

    public FileLock getFileLock() {
        return fileLock;
    }

    public void setFileLock(FileLock fileLock) {
        this.fileLock = fileLock;
    }

    public ReentrantReadWriteLock getMu() {
        return mu;
    }

    public boolean isClosed() {
        return closed;
    }

    public void setClosed(boolean closed) {
        this.closed = closed;
    }

    public boolean isMergeRunning() {
        return mergeRunning;
    }

    public void setMergeRunning(boolean mergeRunning) {
        this.mergeRunning = mergeRunning;
    }

    public DB getDb() {
        return db;
    }

    public void setDb(DB db) {
        this.db = db;
    }

    public BatchOptions getDefaultBatchOptions() {
        return defaultBatchOptions;
    }

    public SimpleObjectPool<Batch> getBatchPool() {
        return batchPool;
    }

    public void setBatchPool(SimpleObjectPool<Batch> batchPool) {
        this.batchPool = batchPool;
    }

    public ReentrantReadWriteLock getLock() {
        return lock;
    }

    public void setLock(ReentrantReadWriteLock lock) {
        this.lock = lock;
    }

    public SimpleObjectPool<LogRecord> getRecordPool() {
        return recordPool;
    }

    public void setRecordPool(SimpleObjectPool<LogRecord> recordPool) {
        this.recordPool = recordPool;
    }

    public byte[] getEncodeHeader() {
        return encodeHeader;
    }

    public void setEncodeHeader(byte[] encodeHeader) {
        this.encodeHeader = encodeHeader;
    }

    public BlockingQueue<Event> getWatchCh() {
        return watchCh;
    }

    public void setWatchCh(BlockingQueue<Event> watchCh) {
        this.watchCh = watchCh;
    }

    public Watcher getWatcher() {
        return watcher;
    }

    public void setWatcher(Watcher watcher) {
        this.watcher = watcher;
    }

    public byte[] getExpiredCursorKey() {
        return expiredCursorKey;
    }

    public void setExpiredCursorKey(byte[] expiredCursorKey) {
        this.expiredCursorKey = expiredCursorKey;
    }

    public ScheduledExecutorService getCronScheduler() {
        return cronScheduler;
    }

    public void setCronScheduler(ScheduledExecutorService cronScheduler) {
        this.cronScheduler = cronScheduler;
    }
}
