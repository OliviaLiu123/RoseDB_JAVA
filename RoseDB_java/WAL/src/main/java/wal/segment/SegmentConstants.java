package main.java.wal.segment;
//用于于存放 WAL 段文件相关的常量
public class SegmentConstants {

    public static final int CHUNCK_HEADER_SIZE = 7;// 块头部大小（校验和、长度和类型），共 7 字节
    public static final int BLOCK_SIZE = 32*1024;// 块大小，32 KB

    public static final int FILE_MODE_PERM = 0644;// 文件权限，等价于 0644（所有者有读写权限，其他用户有读权限）

    // 最大长度，计算和存储 SegmentId、BlockNumber、ChunkOffset、ChunkSize 的总大小
    public static final int MAX_LEN = (Integer.BYTES * 3) + Long.BYTES;
}
