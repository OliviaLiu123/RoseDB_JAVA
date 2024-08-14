package main.java.com.rosedb.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.atomic.AtomicLong;
import java.nio.file.Path;
import java.nio.file.Paths;
//处理与文件相关的操作
public class FileUtils {

    // 获取目录的总大小

    public static long getDirSize(String dirPath) throws IOException{
        AtomicLong size = new AtomicLong(0); //为了线程安全用了AtomicLong,通过原子操作来保证线程安全
        Files.walk(Paths.get(dirPath)).filter(p->!Files.isDirectory(p)).forEach(p->{
            try {
                size.addAndGet(Files.readAttributes(p, BasicFileAttributes.class).size());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        return size.get();
    }
}
