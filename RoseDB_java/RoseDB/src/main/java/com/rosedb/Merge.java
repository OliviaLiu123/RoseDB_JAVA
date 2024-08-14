package main.java.com.rosedb;


import main.java.com.rosedb.db.DB;
import main.java.com.rosedb.index.Indexer;
import main.java.com.rosedb.index.MemoryTreeMap;
import main.java.com.rosedb.options.Options;
import main.java.com.rosedb.record.LogRecord;
import main.java.com.rosedb.record.RecordConstants;
import org.apache.commons.lang3.tuple.Pair;
import main.java.wal.segment.ChunkPosition;
import main.java.wal.writeAheadLog.Reader;
import main.java.wal.writeAheadLog.WriteAheadLog;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static main.java.com.rosedb.record.LogRecord.*;

import static main.java.wal.options.Constants.GB;

public class Merge {
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private static final String MERGE_DIR_SUFFIX_NAME = "-merge";
    private static final int MERGE_FINISHED_BATCH_ID = 0;
    private DB db;
    String hintFileNameSuffix = ".hint";
    private static final String mergeFinNameSuffix = ".mergefin";


    public Merge(DB db) {
        this.db = db;
    }


//1 Merge 函数用于执行数据库的合并操作。它会首先调用 doMerge 函数执行合并操作，如果成功，并且需要重新打开数据库文件（reopenAfterDone 为 true），
// 它将关闭当前文件，替换原始文件，重新打开数据文件并重建索引。
    public synchronized void merge(boolean reopenAfterDone) throws Exception {
        // 执行合并操作。如果合并操作没有成功返回，直接退出该方法
        if (!doMerge()) {
            return;
        }

        // 如果合并完成后不需要重新打开数据库文件，则直接返回
        if (!reopenAfterDone) {
            return;
        }

        // 加写锁，确保在进行文件操作时没有其他线程可以修改数据库
        lock.writeLock().lock();
        try {
            // 关闭当前的文件句柄
            db.closeFiles();

            // 加载合并后的文件，将合并的文件替换为原来的数据文件
            if (!loadMergeFiles(db.getOptions().getDirPath())) {
                throw new IOException("Failed to load merge files");
            }

            // 重新打开数据文件
            db.openWalFiles();

            // 创建一个新的索引器，并重建数据库索引
            Indexer index = new MemoryTreeMap();

            if (!db.loadIndex()) {
                throw new IOException("Failed to load index");
            }
        } finally {
            // 释放写锁，允许其他线程访问数据库
            lock.writeLock().unlock();
        }
    }



//2 执行实际的合并操作。它会遍历旧的段文件，将有效的数据写入新的段文件，并在操作结束时标记合并操作完成
    private boolean doMerge() throws Exception {
        System.out.println("call do merge method ....");
        lock.writeLock().lock();
        try {
            if (db.isMergeRunning()) {
                throw new IllegalStateException("Merge operation is already running");
            }
            db.setMergeRunning(true);
        } finally {
            lock.writeLock().unlock();
        }

        int prevActiveSegId = db.getDataFiles().getActiveSegment().getSegmentId();
        if (!db.getDataFiles().openNewActiveSegment()) {
            return false;
        }

        DB mergeDB = openMergeDB();

        ByteBuffer buf = ByteBuffer.allocate(1024);  // 示例缓冲区大小
        if (buf == null) {
            throw new IllegalStateException("Buffer is null. Cannot proceed with the operation.");
        }
        System.out.println("Buffer initialized successfully.");
        long now = System.nanoTime();

        Reader reader = db.getDataFiles().newReaderWithMax(prevActiveSegId);
        while (true) {
            buf.clear();
            Pair<byte[], ChunkPosition> chunkPair = reader.next();
            if (chunkPair == null) {
                System.out.println("No more chunks to process.");
                break;
            }

            LogRecord record = decodeLogRecord(chunkPair.getKey());
            if (record.getType() == RecordConstants.LOG_RECORD_NORMAL && (record.getExpire() == 0 || record.getExpire() > now)) {
                ChunkPosition indexPos = db.getIndex().get(record.getKey());
                if (indexPos != null && positionEquals(indexPos, chunkPair.getValue())) {
                    record.setBatchId(MERGE_FINISHED_BATCH_ID);
                    ChunkPosition newPosition = mergeDB.getDataFiles().write(record.encodeLogRecord(mergeDB.getEncodeHeader(), buf));
                    mergeDB.getHintFile().write(encodeHintRecord(record.getKey(), newPosition));

                }
            }
        }
        System.out.println("Starting to create merge finish file ....");
        WriteAheadLog mergeFinFile = openMergeFinishedFile();

        if (mergeFinFile == null) {
            System.err.println("Failed to open merge finish file.");
            return false;
        }

        // 打印创建成功的消息
        System.out.println("Merge finish file created successfully.");


        mergeFinFile.write(encodeMergeFinRecord(prevActiveSegId));
        mergeFinFile.close();

        return true;
    }


//3 打开一个用于合并操作的临时数据库。它删除原有的合并目录，创建新的目录，并返回一个新的 DB 实例。
public DB openMergeDB() throws IOException {
    // 生成合并目录路径
    String mergePath = mergeDirPath(db.getOptions().getDirPath());
    // 删除现有的合并目录
    Files.deleteIfExists(Paths.get(mergePath));

    // 修改数据库选项
    Options options = db.getOptions().clone();
    options.setSync(false);
    options.setBytesPerSync(0);
    options.setDirPath(mergePath);

    // 打开新的数据库实例
    DB mergeDB = DB.open(options);
    if (mergeDB == null) {
        throw new IOException("Failed to open merge database");
    }

    // 打开 Hint 文件
    main.java.wal.options.Options walOptions = new main.java.wal.options.Options(
            db.getOptions().getDirPath(),          // 文件路径来自 RoseDB 的 Options
            Long.MAX_VALUE,                // 设置最大段文件大小
            hintFileNameSuffix,            // 段文件后缀
            false,                         // 同步选项
            0                              // 每字节同步数量
    );



    WriteAheadLog hintFile = WriteAheadLog.open(walOptions);

    if (hintFile == null) {
        throw new IOException("Failed to open hint file");
    }
    mergeDB.setHintFile(hintFile);

    // 返回合并数据库实例
    return mergeDB;
}




    //4 生成合并操作的临时目录路径。
    private static String mergeDirPath(String dirPath) {
        return Paths.get(dirPath).getParent().resolve(Paths.get(dirPath).getFileName() + MERGE_DIR_SUFFIX_NAME).toString();
    }



//5.打开表示合并操作完成的文件

    private WriteAheadLog openMergeFinishedFile() throws IOException {
        System.out.println("openMergeFinishedFile method called");
        Path mergeFinFilePath = Paths.get(db.getOptions().getDirPath(), "000000001.MERGEFIN");
        System.out.println("Merge finish file path at: " + mergeFinFilePath.toAbsolutePath());
        if (!Files.exists(mergeFinFilePath)) {
            System.err.println("Merge finish file path not found: " + mergeFinFilePath.toAbsolutePath());
            throw new IOException("Merge finish file not found: " + mergeFinFilePath.toAbsolutePath());
        }
        // Assuming mergeFinNameSuffix is a string that should match with file

        try {
            WriteAheadLog file = WriteAheadLog.open(new main.java.wal.options.Options(db.getOptions().getDirPath(), GB, mergeFinNameSuffix, false, 0));

            // 打印成功信息
            System.out.println("Merge finish file opened successfully.");

            return file;
        } catch (IOException e) {
            System.err.println("Failed to open WriteAheadLog: " + e.getMessage());
            throw e;
        }
    }


    //6.比较两个 ChunkPosition 对象是否相同
    private boolean positionEquals(ChunkPosition a, ChunkPosition b) {
    return a.getSegmentId() == b.getSegmentId() && a.getBlockNumber() == b.getBlockNumber() && a.getChunkOffset() == b.getChunkOffset();
}

//7.加载合并后的文件，并将合并数据复制到原始数据目录中
    public static boolean loadMergeFiles(String dirPath) throws IOException {
        String mergeDirPath = mergeDirPath(dirPath);
        System.out.println(" merge directory path: " + mergeDirPath);
        if (!Files.exists(Paths.get(mergeDirPath))) {
            return false;
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                Files.deleteIfExists(Paths.get(mergeDirPath));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));

        int mergeFinSegmentId = getMergeFinSegmentId(mergeDirPath);
        for (int fileId = 1; fileId <= mergeFinSegmentId; fileId++) {
            String destFile = WriteAheadLog.segmentFileName(dirPath, ".SEG", fileId);
            Files.deleteIfExists(Paths.get(destFile));
            copyFile(mergeDirPath, dirPath, ".SEG", fileId);
        }

        copyFile(mergeDirPath, dirPath, ".MERGEFIN", 1);
        copyFile(mergeDirPath, dirPath, ".HINT", 1);

        return true;
    }

    //
    private static void copyFiles(String mergeDirPath, String dirPath) throws IOException {
        int mergeFinSegmentId = getMergeFinSegmentId(mergeDirPath);
        for (int fileId = 1; fileId <= mergeFinSegmentId; fileId++) {
            String destFile = WriteAheadLog.segmentFileName(dirPath, ".SEG", fileId);
            Files.deleteIfExists(Paths.get(destFile));  // 删除原始数据文件
            copyFile(mergeDirPath, dirPath, ".SEG", fileId);  // 将合并数据文件移动到原始数据目录
        }

        copyFile(mergeDirPath, dirPath, ".MERGEFIN", 1);  // 复制MERGEFINISHED文件
        copyFile(mergeDirPath, dirPath, ".HINT", 1);  // 复制HINT文件
    }
//    private static void copyFile(String srcDir, String destDir, String suffix, int fileId) throws IOException {
//        Path srcFile = Paths.get(WriteAheadLog.segmentFileName(srcDir, suffix, fileId));
//        if (!Files.exists(srcFile)) {
//            System.err.println("Source file not found: " + srcFile.toString());
//            return;
//        }
//
//        Path destFile = Paths.get(WriteAheadLog.segmentFileName(destDir, suffix, fileId));
//        Files.move(srcFile, destFile, StandardCopyOption.REPLACE_EXISTING);
//        System.out.println("Moved file " + srcFile.toString() + " to " + destFile.toString());
//    }

    private static void copyFile(String srcDir, String destDir, String suffix, int fileId) throws IOException {
        Path srcFile = Paths.get(WriteAheadLog.segmentFileName(srcDir, suffix, fileId));
//        if (Files.exists(srcFile)) {
//            Path destFile = Paths.get(WriteAheadLog.segmentFileName(destDir, suffix, fileId));
//            Files.move(srcFile, destFile, StandardCopyOption.REPLACE_EXISTING);
//        }

        if (!Files.exists(srcFile)) {
            System.err.println("Source file not found: " + srcFile.toString());
            return;
        }

        Path destFile = Paths.get(WriteAheadLog.segmentFileName(destDir, suffix, fileId));
        Files.move(srcFile, destFile, StandardCopyOption.REPLACE_EXISTING);
        System.out.println("Moved file " + srcFile.toString() + " to " + destFile.toString());
    }




    //8.获取合并完成文件的段 Id
//    public static int getMergeFinSegmentId(String mergePath) throws IOException {
//        Path mergeFinFile = Paths.get(WriteAheadLog.segmentFileName(mergePath, ".MERGEFIN", 1));
//        try (DataInputStream dis = new DataInputStream(Files.newInputStream(mergeFinFile))) {
//            dis.skipBytes(7);  // 跳过块头
//            return dis.readInt();
//        }
//    }

    public static int getMergeFinSegmentId(String mergePath) throws IOException {
        // Construct the path for the merge finish file
        Path mergeFinFile = Paths.get(WriteAheadLog.segmentFileName(mergePath, ".MERGEFIN", 1));
        System.out.println("for mergeFinFile, mergePath is: "+ mergeFinFile.toAbsolutePath());
        // Print the path for debugging
        System.out.println("Attempting to read the merge finish file at: " + mergeFinFile.toAbsolutePath());

        // Check if the file exists before attempting to read
        if (!Files.exists(mergeFinFile)) {
            System.err.println("Merge finish file not found in getMergefinSegmentId method : " + mergeFinFile.toAbsolutePath());
            throw new IOException("Merge finish file not found getMergefinSegmentId method: " + mergeFinFile.toAbsolutePath());
        }

        try (DataInputStream dis = new DataInputStream(Files.newInputStream(mergeFinFile))) {
            dis.skipBytes(7);  // Skip header
            int segmentId = dis.readInt();

            // Print the segment ID for debugging
            System.out.println("Read segment ID: " + segmentId);

            return segmentId;
        } catch (IOException e) {
            // Print detailed error message
            System.err.println("Error reading the merge finish file: " + e.getMessage());
            throw e;
        }
    }




    //9.从提示文件（Hint File）中加载索引
    public void loadIndexFromHintFile() throws Exception {
        // 打开 Hint 文件
        WriteAheadLog hintFile = WriteAheadLog.open(new main.java.wal.options.Options(
                db.getOptions().getDirPath(),
                Long.MAX_VALUE,  // 用于设置最大段文件大小
                hintFileNameSuffix,  // 段文件后缀
                false,  // 同步选项
                0  // 每字节同步数量
        ));

        try {
            Reader reader = hintFile.newReader();
            hintFile.setIsStartupTraversal(true);

            while (true) {
                Pair<byte[], ChunkPosition> chunk = reader.next();
                if (chunk == null) {
                    break;
                }

                // 使用解码方法将 chunk 分别解码为 key 和 position
                byte[] key = chunk.getKey();  // 从 Pair 中获取 key
                ChunkPosition position = chunk.getValue();  // 从 Pair 中获取位置

                // 将 key 和 position 存入索引中
                db.getIndex().put(key, position);
            }

            hintFile.setIsStartupTraversal(false);
        } finally {
            hintFile.close();
        }
    }





}
