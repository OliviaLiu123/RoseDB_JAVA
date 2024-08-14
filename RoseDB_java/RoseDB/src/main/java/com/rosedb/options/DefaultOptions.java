package main.java.com.rosedb.options;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class DefaultOptions {
    public static final main.java.com.rosedb.options.Options DEFAULT_OPTIONS;

    static {
        DEFAULT_OPTIONS = new main.java.com.rosedb.options.Options(
                tempDBDir(),              // DirPath: 使用系统临时目录
                1 * Constants.GB,         // SegmentSize: 默认 1GB
                false,                    // Sync: 默认不同步
                0,                        // BytesPerSync: 默认 0 字节
                0,                        // WatchQueueSize: 默认 0
                ""                        // AutoMergeCronExpr: 默认空
        );
    }

    public static final BatchOptions DEFAULT_BATCH_OPTIONS = new BatchOptions(true, false);

    private static String tempDBDir() {
        try {
            Path tempDir = Files.createTempDirectory("rosedb-temp");
            return tempDir.toString();
        } catch (IOException e) {
            throw new RuntimeException("Failed to create temp directory", e);
        }
    }
}
