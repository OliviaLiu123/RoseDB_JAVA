package main.java.com.rosedb.options;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Options implements Cloneable{
    private String dirPath;  // DirPath specifies the directory path where the WAL segment files will be stored.
    private long segmentSize;  // SegmentSize specifies the maximum size of each segment file in bytes.
    private boolean sync;  // Sync is whether to synchronize writes through OS buffer cache and down onto the actual disk.
    private int bytesPerSync;  // BytesPerSync specifies the number of bytes to write before calling fsync.
    private long watchQueueSize;  // WatchQueueSize the cache length of the watch queue.
    private String autoMergeCronExpr;  // AutoMergeEnable enable the auto merge.

    // Constructor with all parameters
    public Options(String dirPath, long segmentSize, boolean sync, int bytesPerSync, long watchQueueSize, String autoMergeCronExpr) {
        this.dirPath = dirPath;
        this.segmentSize = segmentSize;
        this.sync = sync;
        this.bytesPerSync = bytesPerSync;
        this.watchQueueSize = watchQueueSize;
        this.autoMergeCronExpr = autoMergeCronExpr;
    }
    @Override
    public Options clone() {
        try {
            // 调用Object的clone方法实现浅拷贝
            return (Options) super.clone();
        } catch (CloneNotSupportedException e) {
            // 如果发生异常，处理方式取决于你的应用程序需求
            throw new AssertionError("Cloning not supported", e);
        }
    }
    // Getters and setters
    public String getDirPath() {
        return dirPath;
    }

    public void setDirPath(String dirPath) {
        this.dirPath = dirPath;
    }

    public long getSegmentSize() {
        return segmentSize;
    }

    public void setSegmentSize(long segmentSize) {
        this.segmentSize = segmentSize;
    }

    public boolean isSync() {
        return sync;
    }

    public void setSync(boolean sync) {
        this.sync = sync;
    }

    public int getBytesPerSync() {
        return bytesPerSync;
    }

    public void setBytesPerSync(int bytesPerSync) {
        this.bytesPerSync = bytesPerSync;
    }

    public long getWatchQueueSize() {
        return watchQueueSize;
    }

    public void setWatchQueueSize(long watchQueueSize) {
        this.watchQueueSize = watchQueueSize;
    }

    public String getAutoMergeCronExpr() {
        return autoMergeCronExpr;
    }

    public void setAutoMergeCronExpr(String autoMergeCronExpr) {
        this.autoMergeCronExpr = autoMergeCronExpr;
    }
}
