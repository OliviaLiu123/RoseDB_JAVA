package main.java.com.rosedb.record;

public class RecordConstants {
    public static final byte LOG_RECORD_NORMAL = 0;
    public static final byte LOG_RECORD_DELETED = 1;
    public static final byte LOG_RECORD_BATCH_FINISHED = 2;
    public static final int MAX_LOG_RECORD_HEADER_SIZE = 4 * 2 + 8 * 2 + 1;  // 32-bit和64-bit的变长整数 + 1字节
}
