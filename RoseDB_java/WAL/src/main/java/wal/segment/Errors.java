package main.java.wal.segment;
/**
 * 定义一些错误类型，用于表示操作过程中可能出现的异常情况。
 */

    public class Errors {
        public static final Exception ERR_CLOSED = new Exception("The segment file is closed.");
        public static final Exception ERR_INVALID_CRC = new Exception("Invalid CRC, the data may be corrupted.");
    }

