package main.java.wal.options;

public class DefaultOptions {
    public static final main.java.wal.options.Options DEFAULT_OPTIONS;

    static {
        DEFAULT_OPTIONS = new main.java.wal.options.Options();
        DEFAULT_OPTIONS.setDirPath(System.getProperty("java.io.tmpdir")); // 使用系统临时目录
        DEFAULT_OPTIONS.setSegmentSize(main.java.wal.options.Constants.GB); // 默认 1GB
        DEFAULT_OPTIONS.setSegmentFileExt(".SEG"); // 默认文件扩展名
        DEFAULT_OPTIONS.setSync(false); // 默认不同步
        DEFAULT_OPTIONS.setBytesPerSync(0); // 默认 0 字节
    }
}
