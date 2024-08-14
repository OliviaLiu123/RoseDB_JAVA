package main.java.com.rosedb;

//批量处理

import cn.hutool.core.lang.Snowflake;
import cn.hutool.core.util.IdUtil;
import main.java.com.rosedb.db.DB;
import main.java.com.rosedb.db.SimpleObjectPool;
import main.java.com.rosedb.options.BatchOptions;
import main.java.com.rosedb.record.LogRecord;
import main.java.com.rosedb.record.RecordConstants;
import main.java.com.rosedb.utils.HashUtils;
import main.java.com.rosedb.watch.Event;
import main.java.com.rosedb.watch.WatchActionType;
import main.java.wal.segment.BufferPool;
import main.java.wal.segment.ChunkPosition;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Batch {
    private  DB db;
    private List<LogRecord> pendingWrites; // 保存待写入的数据
    private Map<Long, List<Integer>> pendingWritesMap; // 映射记录的哈希键到索引，快速查找 pendingWrites
    private BatchOptions options;
    private ReentrantReadWriteLock mu;
    private boolean committed;
    private boolean rollbacked;
    private Snowflake batchId;
    private List<ByteBuffer> buffers;

    private final BatchOptions defaultBatchOptions = new BatchOptions(false, false);





    // 构造函数，用于创建一个新的 Batch 实例
    public Batch(DB db, BatchOptions options) {
        this.db = db;
        this.options = options;
        this.committed = false;
        this.rollbacked = false;
        this.pendingWrites = new ArrayList<>();
        this.pendingWritesMap = new HashMap<>();
        this.mu = new ReentrantReadWriteLock();
        this.buffers = new ArrayList<>();

        if (!options.isReadOnly()) {
            try {
                this.batchId = IdUtil.getSnowflake(1); // 使用 Hutool 的 Snowflake 生成唯一ID
            } catch (Exception e) {
                throw new RuntimeException("SnowflakeNode initialization failed", e);
            }
        }
        lock();
    }

    public Batch(DB db, BatchOptions options, Snowflake batchId) {
        this.db = db;
        this.options = options;
        this.committed = false;
        this.rollbacked = false;
        this.pendingWrites = new ArrayList<>();
        this.pendingWritesMap = new HashMap<>();
        this.mu= new ReentrantReadWriteLock();
        this.buffers = new ArrayList<>();
        this.batchId = batchId;
    }

    public Batch(DB db) {
    }


    public Batch newBatch(BatchOptions options) {
        Batch batch = new Batch(db, options);
        batch.setCommitted(false);
        batch.setRollbacked(false);

        if (!options.isReadOnly()) {
            try {
                // 使用Hutool的Snowflake生成唯一ID
                Snowflake node = IdUtil.getSnowflake(1);
                batch.setBatchId(node);
            } catch (Exception e) {
                throw new RuntimeException("SnowflakeNode initialization failed", e);
            }
        }
        batch.lock();
        return batch;
    }

    public static Batch createNewBatch(DB db) {
        try {
            Snowflake node = IdUtil.getSnowflake(1);
            BatchOptions defaultOptions = new BatchOptions(false, false); // 这里定义默认的 BatchOptions
            return new Batch(db, defaultOptions, node);
        } catch (Exception e) {
            throw new RuntimeException("SnowflakeNode initialization failed", e);
        }
    }

    public static LogRecord NewRecord() {
        // 创建一个新的LogRecord实例
        return new LogRecord(new byte[0], new byte[0], (byte) 0, 0L, 0L);
    }



    // 用于初始化一个新的 Batch 实例
    public Batch init(boolean readOnly, boolean sync, DB db) {
        this.options.setReadOnly(readOnly);
        this.options.setSync(sync);
        this.db = db;
        lock();
        return this;
    }

    // 重置 Batch 实例
    public void reset() {
        this.db = null;
        this.pendingWrites.clear();
        this.pendingWritesMap.clear();
        this.committed = false;
        this.rollbacked = false;
        this.buffers.clear();
    }

    // 锁定操作
    private void lock() {
        if (this.db == null) {
            throw new IllegalStateException("DB instance is not initialized.");
        }
        if (this.options.isReadOnly()) {
            this.db.getLock().readLock().lock();
        } else {
            this.db.getLock().writeLock().lock();
        }
    }

    // 解锁操作
    private void unlock() {
        if (this.options.isReadOnly()) {
            this.db.getLock().readLock().unlock();
        } else {
            this.db.getLock().writeLock().unlock();
        }
    }

    // 添加键值对到批处理中
    public void put(byte[] key, byte[] value) throws Exception {
        System.out.println("start put method in Batch class....");
        if (key.length == 0) {
            throw new IllegalArgumentException("Key cannot be empty");
        }
        if (this.db.isClosed()) {
            throw new IllegalStateException("DB is closed");
        }
        if (this.options.isReadOnly()) {
            throw new IllegalStateException("Batch is read-only");
        }

        mu.writeLock().lock();
        try {
            System.out.println("Acquired write lock.");

            LogRecord record = lookupPendingWrites(key);
            if (record == null) {
                System.out.println("Record not found in pending writes. Creating new record.");
                record = this.db.getRecordPool().get();
                appendPendingWrites(key, record);
            }else {
                System.out.println("Record found in pending writes.");
            }
            record.setKey(key);
            record.setValue(value);
            record.setType(RecordConstants.LOG_RECORD_NORMAL);
            record.setExpire(0);
            System.out.println("Successfully set key, value, type, and expiration for the record.");
        } finally {
            mu.writeLock().unlock();
        }
    }

    // 添加具有TTL的键值对到批处理中
    public void putWithTTL(byte[] key, byte[] value, Duration ttl) throws Exception {
        if (key.length == 0) {
            throw new IllegalArgumentException("Key cannot be empty");
        }
        if (this.db.isClosed()) {
            throw new IllegalStateException("DB is closed");
        }
        if (this.options.isReadOnly()) {
            throw new IllegalStateException("Batch is read-only");
        }

        mu.writeLock().lock();
        try {
            LogRecord record = lookupPendingWrites(key);
            if (record == null) {
                record = this.db.getRecordPool().get();
                appendPendingWrites(key, record);
            }
            record.setKey(key);
            record.setValue(value);
            record.setType(RecordConstants.LOG_RECORD_NORMAL);
            record.setExpire(System.currentTimeMillis() + ttl.toMillis());

        } finally {
            mu.writeLock().unlock();
        }
    }

    // 从批处理中获取与指定键关联的值
    public byte[] get(byte[] key) throws Exception {
        if (key.length == 0) {
            throw new IllegalArgumentException("Key cannot be empty");
        }
        if (this.db.isClosed()) {
            throw new IllegalStateException("DB is closed");
        }

        mu.readLock().lock();
        try {
            System.out.println("Start get method with key: " + new String(key));

            LogRecord record = lookupPendingWrites(key);
            if (record != null) {
                System.out.println("Found key in pending writes: " + new String(key));
                if (record.getType() == RecordConstants.LOG_RECORD_DELETED || record.isExpired(System.currentTimeMillis())) {
                    System.out.println("Key found in pending writes but marked as deleted or expired: " + new String(key));
                    throw new Exception("Key not found");
                }
                return record.getValue();
            }

            System.out.println("Key not found in pending writes, checking in index: " + new String(key));
            ChunkPosition position = this.db.getIndex().get(key);
            if (position == null) {
                System.out.println("Key not found in index: " + new String(key));
                throw new Exception("Key not found");
            }



            byte[] chunk = this.db.getDataFiles().read(position);
            if (chunk == null || chunk.length == 0) {
                System.out.println("Error: Chunk data is null or empty.");
                return null; // 或者抛出异常，或进行其他处理
            }

            record = LogRecord.decodeLogRecord(chunk);
            if (record.getType() == RecordConstants.LOG_RECORD_DELETED || record.isExpired(System.currentTimeMillis())) {
                System.out.println("Key found in WAL but marked as deleted or expired: " + new String(key));
                this.db.getIndex().delete(key);
                throw new Exception("Key not found");
            }

            System.out.println("Key successfully retrieved: " + new String(key));
            return record.getValue();
        } finally {
            mu.readLock().unlock();
            System.out.println("End get method for key: " + new String(key));
        }
    }

    // 标记键为删除状态
    public void delete(byte[] key) throws Exception {
        if (key.length == 0) {
            throw new IllegalArgumentException("Key cannot be empty");
        }
        if (this.db.isClosed()) {
            throw new IllegalStateException("DB is closed");
        }
        if (this.options.isReadOnly()) {
            throw new IllegalStateException("Batch is read-only");
        }

        mu.writeLock().lock();
        try {
            LogRecord record = lookupPendingWrites(key);
            if (record == null) {
                record = new LogRecord(new byte[0], new byte[0], (byte) 0, 0L, 0L);
                appendPendingWrites(key, record);
            }
            record.setType(RecordConstants.LOG_RECORD_DELETED);
            record.setExpire(0);
            record.setValue(null);
        } finally {
            mu.writeLock().unlock();
        }
    }

    // 检查键是否存在
    public boolean exist(byte[] key) throws Exception {
        if (key.length == 0) {
            throw new IllegalArgumentException("Key cannot be empty");
        }
        if (this.db.isClosed()) {
            throw new IllegalStateException("DB is closed");
        }

        mu.readLock().lock();
        try {
            LogRecord record = lookupPendingWrites(key);
            if (record != null) {
                return record.getType() != RecordConstants.LOG_RECORD_DELETED && !record.isExpired(System.currentTimeMillis());
            }

            ChunkPosition position = this.db.getIndex().get(key);
            if (position == null) {
                return false;
            }

            byte[] chunk = this.db.getDataFiles().read(position);
            record = LogRecord.decodeLogRecord(chunk);
            return record.getType() != RecordConstants.LOG_RECORD_DELETED && !record.isExpired(System.currentTimeMillis());
        } finally {
            mu.readLock().unlock();
        }
    }

    // 设置键的TTL
    public void expire(byte[] key, Duration ttl) throws Exception {
        if (key.length == 0) {
            throw new IllegalArgumentException("Key cannot be empty");
        }
        if (this.db.isClosed()) {
            throw new IllegalStateException("DB is closed");
        }
        if (this.options.isReadOnly()) {
            throw new IllegalStateException("Batch is read-only");
        }

        mu.writeLock().lock();
        try {
            LogRecord record = lookupPendingWrites(key);
            if (record == null) {
                ChunkPosition position = this.db.getIndex().get(key);
                if (position == null) {
                    throw new Exception("Key not found");
                }
                byte[] chunk = this.db.getDataFiles().read(position);
                record = LogRecord.decodeLogRecord(chunk);
                if (record.getType() == RecordConstants.LOG_RECORD_DELETED || record.isExpired(System.currentTimeMillis())) {
                    this.db.getIndex().delete(key);
                    throw new Exception("Key not found");
                }
                appendPendingWrites(key, record);
            }
            record.setExpire(System.currentTimeMillis() + ttl.toMillis());

        } finally {
            mu.writeLock().unlock();
        }
    }

    // 获取键的TTL
    public Duration ttl(byte[] key) throws Exception {
        if (key.length == 0) {
            throw new IllegalArgumentException("Key cannot be empty");
        }
        if (this.db.isClosed()) {
            throw new IllegalStateException("DB is closed");
        }

        mu.readLock().lock();
        try {
            LogRecord record = lookupPendingWrites(key);
            if (record != null) {
                if (record.getExpire() == 0) {
                    return null;
                }
                return Duration.ofMillis(record.getExpire() - System.currentTimeMillis());
            }

            ChunkPosition position = this.db.getIndex().get(key);
            if (position == null) {
                throw new Exception("Key not found");
            }

            byte[] chunk = this.db.getDataFiles().read(position);
            record = LogRecord.decodeLogRecord(chunk);
            if (record.getType() ==RecordConstants.LOG_RECORD_DELETED || record.isExpired(System.currentTimeMillis())) {
                this.db.getIndex().delete(key);
                throw new Exception("Key not found");
            }

            return record.getExpire() == 0 ? null : Duration.ofMillis(record.getExpire() - System.currentTimeMillis());
        } finally {
            mu.readLock().unlock();
        }
    }

    // 移除键的TTL
    public void persist(byte[] key) throws Exception {
        if (key.length == 0) {
            throw new IllegalArgumentException("Key cannot be empty");
        }
        if (this.db.isClosed()) {
            throw new IllegalStateException("DB is closed");
        }
        if (this.options.isReadOnly()) {
            throw new IllegalStateException("Batch is read-only");
        }

        mu.writeLock().lock();
        try {
            LogRecord record = lookupPendingWrites(key);
            if (record == null) {
                ChunkPosition position = this.db.getIndex().get(key);
                if (position == null) {
                    throw new Exception("Key not found");
                }
                byte[] chunk = this.db.getDataFiles().read(position);
                record = LogRecord.decodeLogRecord(chunk);
                if (record.getType() ==RecordConstants.LOG_RECORD_DELETED || record.isExpired(System.currentTimeMillis())) {
                    this.db.getIndex().delete(key);
                    throw new Exception("Key not found");
                }
                appendPendingWrites(key, record);
            }
            record.setExpire(0);
        } finally {
            mu.writeLock().unlock();
        }
    }

    // 提交批处理，写入数据
    public void commit() throws Exception {
        System.out.println("commit start ....");
        unlock(); // 释放读/写锁
        if (this.db.isClosed()) {
            throw new IllegalStateException("DB is closed");
        }

        if (this.options.isReadOnly() || this.pendingWrites.isEmpty()) {
            return;
        }

        mu.writeLock().lock(); // 获取写锁
        try {
            if (this.committed) {
                throw new IllegalStateException("Batch already committed");
            }
            if (this.rollbacked) {
                throw new IllegalStateException("Batch already rollbacked");
            }

            long batchId = this.batchId.nextId();
            long now = System.currentTimeMillis();


            // 写入WAL缓冲区
            for (LogRecord record : this.pendingWrites) {
                byte[] buf = BufferPool.getBuffer(); // 使用 BufferPool 来获取缓冲区
                System.out.println("Acquired buffer from BufferPool: ");

                record.setBatchId(batchId);
                byte[] encRecord = record.encodeLogRecord(this.db.getEncodeHeader(), ByteBuffer.wrap(buf));

                // 打印编码后的日志记录
                System.out.println("Encoded LogRecord: " );


                this.db.getDataFiles().pendingWrites(encRecord);
                this.buffers.add(ByteBuffer.wrap(buf));

                // 确认将数据添加到缓冲区
                System.out.println("Buffer content after encoding: ");
                System.out.println("Buffer added to buffers list.");
            }
            System.out.println("Contents of pendingWrites in DataFiles: " );
            for (LogRecord record : pendingWrites) {
                System.out.println("LogRecord Details: " + record);

                // 如果 LogRecord 类没有覆盖 toString() 方法，可以手动打印各个属性
                System.out.println("Key: " + new String(record.getKey()));
                System.out.println("Value: " + new String(record.getValue()));
                System.out.println("Type: " + record.getType());
                System.out.println("BatchId: " + record.getBatchId());
                System.out.println("Expire Time: " + record.getExpire());
            }




// 检查 buffers 列表中的内容
            System.out.println("Buffers list size: " + this.buffers.size());
            for (ByteBuffer buffer : this.buffers) {
                System.out.println("Buffer in list: ");
            }
//批处理操作（batch operation）结束时，向 Write-Ahead Log (WAL) 添加一个特殊的记录，以标识批处理的结束。这对于保证数据的一致性和完整性非常重要
            // 写入一个记录以指示批处理结束
            // 创建一个 LogRecord 实例，使用空的 key 和 value
            byte[] emptyKey = new byte[0];
            byte[] emptyValue = new byte[0];
            LogRecord endRecord = new LogRecord(emptyKey, emptyValue, RecordConstants.LOG_RECORD_BATCH_FINISHED, batchId, 0);

            // 调用实例方法来编码记录
            ByteBuffer endBuf = ByteBuffer.allocate(128); // 为endRecord单独创建一个缓冲区
            byte[] endEncRecord = endRecord.encodeLogRecord(this.db.getEncodeHeader(), endBuf);

            // 将编码后的记录添加到数据文件中
            this.db.getDataFiles().pendingWrites(endEncRecord);
            // 添加日志语句以确认写入完成
            System.out.println("Batch end record written to WAL successfully. Record details: " + Arrays.toString(endEncRecord));

            // 将缓冲区添加到buffers列表中
            this.buffers.add(endBuf);


            System.out.println("starting write data on chuckpostion..");
            // 将所有记录写入WAL文件
            List<ChunkPosition> chunkPositions = this.db.getDataFiles().writeAll();

            // 打印调试 chunkPositions 里面的内容
            System.out.println("Chunk positions after writeAll: ");
            for (int i = 0; i < chunkPositions.size(); i++) {
                ChunkPosition pos = chunkPositions.get(i);
                System.out.println("ChunkPosition " + i + ":");
                System.out.println("  Segment ID: " + pos.getSegmentId());
                System.out.println("  Block Number: " + pos.getBlockNumber());
                System.out.println("  Chunk Offset: " + pos.getChunkOffset());
                System.out.println("  Chunk Size: " + pos.getChunkSize());
            }
///////





            if (chunkPositions.size() != this.pendingWrites.size() + 1) {
                throw new IllegalStateException("Chunk positions length does not match pending writes length");
            }

            // 如果需要，刷新WAL文件
            if (this.options.isSync() && !this.db.getOptions().isSync()) {
                this.db.getDataFiles().sync();
            }

            // 更新索引
            for (int i = 0; i < this.pendingWrites.size(); i++) {
                LogRecord record = this.pendingWrites.get(i);
                if (record.getType() == RecordConstants.LOG_RECORD_DELETED || record.isExpired(now)) {
                    this.db.getIndex().delete(record.getKey());
                } else {
                    this.db.getIndex().put(record.getKey(), chunkPositions.get(i));
                }

                if (this.db.getOptions().getWatchQueueSize() > 0) {
                    WatchActionType actionType = (record.getType() == RecordConstants.LOG_RECORD_DELETED) ? WatchActionType.DELETE : WatchActionType.PUT;
                    Event e = new Event(actionType, record.getKey(), record.getValue(), record.getBatchId());
                    this.db.getWatcher().putEvent(e);
                }


                this.db.getRecordPool().put(record);
            }

            this.committed = true;
        } finally {
            mu.writeLock().unlock();
        }
    }


    // 回滚未提交的批处理
    public void rollback() throws Exception {
        unlock();

        if (this.db.isClosed()) {
            throw new IllegalStateException("DB is closed");
        }

        mu.writeLock().lock();
        try {
            if (this.committed) {
                throw new IllegalStateException("Batch already committed");
            }
            if (this.rollbacked) {
                throw new IllegalStateException("Batch already rollbacked");
            }

            for (ByteBuffer buf : this.buffers) {
                // 释放ByteBuffer（在Java中可能不需要手动释放）
            }

            if (!this.options.isReadOnly()) {
                // 清除待处理的写操作
                for (LogRecord record : this.pendingWrites) {
                    this.db.getRecordPool().put(record);
                }
                this.pendingWrites.clear();
                this.pendingWritesMap.clear();
            }

            this.rollbacked = true;
        } finally {
            mu.writeLock().unlock();
        }
    }

    // 在pendingWrites中查找记录
    private LogRecord lookupPendingWrites(byte[] key) {
        if (this.pendingWritesMap.isEmpty()) {
            return null;
        }

        long hashKey = HashUtils.memHash(key);
        List<Integer> entries = this.pendingWritesMap.get(hashKey);
        if (entries != null) {
            for (int entry : entries) {
                if (Arrays.equals(this.pendingWrites.get(entry).getKey(), key)) {
                    return this.pendingWrites.get(entry);
                }
            }
        }
        return null;
    }

    // 添加新的记录到pendingWrites和pendingWritesMap
    private void appendPendingWrites(byte[] key, LogRecord record) {
        this.pendingWrites.add(record);
        long hashKey = HashUtils.memHash(key);
        this.pendingWritesMap.computeIfAbsent(hashKey, k -> new ArrayList<>()).add(this.pendingWrites.size() - 1);
    }

    //getter and setter

    public DB getDb() {
        return db;
    }

    public void setDb(DB db) {
        this.db = db;
    }

    public List<LogRecord> getPendingWrites() {
        return pendingWrites;
    }

    public void setPendingWrites(List<LogRecord> pendingWrites) {
        this.pendingWrites = pendingWrites;
    }

    public Map<Long, List<Integer>> getPendingWritesMap() {


        return pendingWritesMap;
    }

    public void setPendingWritesMap(Map<Long, List<Integer>> pendingWritesMap) {
        this.pendingWritesMap = pendingWritesMap;
    }

    public BatchOptions getOptions() {
        return options;
    }

    public void setOptions(BatchOptions options) {
        this.options = options;
    }

    public ReentrantReadWriteLock getLock() {
        return mu;
    }

    public void setLock(ReentrantReadWriteLock lock) {
        this.mu = lock;
    }

    public boolean isCommitted() {
        return committed;
    }

    public void setCommitted(boolean committed) {
        this.committed = committed;
    }

    public boolean isRollbacked() {
        return rollbacked;
    }

    public void setRollbacked(boolean rollbacked) {
        this.rollbacked = rollbacked;
    }

    public Snowflake getBatchId() {
        return batchId;
    }

    public void setBatchId(Snowflake batchId) {
        this.batchId = batchId;
    }

    public List<ByteBuffer> getBuffers() {
        return buffers;
    }

    public void setBuffers(List<ByteBuffer> buffers) {
        this.buffers = buffers;
    }
}
