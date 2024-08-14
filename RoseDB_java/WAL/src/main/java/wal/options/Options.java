package main.java.wal.options;


// 这个类是WriteAheadLog的配置选项

public class Options  {
    //DirPath 指定WAL 段文件存储的目录路径
    private String dirPath;

    //指定每个段文件的最大大小以字节为单位
    private long segmentSize;
/*segmentFileExt 指文件的文件扩展名
文件扩展名必须以“。”开头，默认值是“。SEG”
它用于识别目录中的不同类型文件，目前被roseDB 来识别段文件和提示文件
对于大多数用户来说，这不是一个常见的方法
 */
    private String segmentFileExt;

/*
sync 指定是否是否通过操作系统缓冲区同步写入并将其保存到实际磁盘上,设置sync是确保单个写入操作持久性
的必要条件，但也会导致写入速度很慢
如果为false, 且机器崩溃，则可能丢失一些最近的写入，请注意如果只是进程崩溃（机器未崩溃）。则不会丢失任何写入
sync 为false 的语义与write 系统调用小童，
sync 为true 时 意味着写入后调用fsync
 */

    private boolean sync;

    // BytesPerSync 指定在调用 fsync 之前要写入的字节数。
    private int bytesPerSync;

    // construct
    public Options(){
        this.dirPath=System.getProperty("java.io.tmpdir");//设置为临时目录
        this.segmentSize=1024*1024*1024;
        this.segmentFileExt=".SEG";
        this.sync = false;
        this.bytesPerSync =0;
    }

    public Options(String dirPath, long segmentSize, String segmentFileExt, boolean sync, int bytesPerSync) {
        this.dirPath = dirPath;
        this.segmentSize = segmentSize;
        this.segmentFileExt = segmentFileExt;
        this.sync = sync;
        this.bytesPerSync = bytesPerSync;
    }

    //getter and setter
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

    public String getSegmentFileExt() {
        return segmentFileExt;
    }

    public void setSegmentFileExt(String segmentFileExt) {
        this.segmentFileExt = segmentFileExt;
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
}

