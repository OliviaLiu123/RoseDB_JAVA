package main.java.wal.segment;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

//通过 ConcurrentLinkedQueue 实现了一个简单的对象池，用于管理缓冲区的复用，从而减少频繁分配和回收内存的开销
public class BufferPool {
    private static final int BLOCK_SIZE = 32 * 1024; // 假设 blockSize 是 32KB
    private static final ConcurrentLinkedQueue<byte[]> pool = new ConcurrentLinkedQueue<>();

    // 获取一个缓冲区
    public static byte[] getBuffer() {
        byte[] buffer = pool.poll(); // 从池中取出一个缓冲区
        if (buffer == null) {
            buffer = new byte[BLOCK_SIZE]; // 如果池中没有缓冲区，则创建一个新的
        }
        return buffer;
    }

    // 将缓冲区放回池中
    public static void putBuffer(byte[] buffer) {
        if (buffer != null && buffer.length == BLOCK_SIZE) {
            pool.offer(buffer); // 将缓冲区放回池中供再次使用
        }
    }
}

