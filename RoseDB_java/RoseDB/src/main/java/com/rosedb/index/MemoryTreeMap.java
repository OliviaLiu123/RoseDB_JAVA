package main.java.com.rosedb.index;

import main.java.wal.segment.ChunkPosition;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.Arrays;

public class MemoryTreeMap implements Indexer {

    private final TreeMap<byte[], ChunkPosition> tree;  // 使用TreeMap来模拟B树
    private final ReentrantReadWriteLock lock;  // 用于读写锁

    public MemoryTreeMap() {
        this.tree = new TreeMap<>((a, b) -> compareByteArrays(a, b)); // 使用字节数组的比较器
        this.lock = new ReentrantReadWriteLock();
    }
    // 实现字节数组比较
public int compareByteArrays(byte[] a, byte[] b){

    if (a==b) return 0;
    if(a==null) return -1;
    if(b==null) return 1;

    int length = Math.min(a.length, b.length);
    for (int i =0; i< length; i++){
        int diff = Byte.toUnsignedInt(a[i]) - Byte.toUnsignedInt(b[i]);
        if (diff != 0) {
            return diff;
        }
    }
    return a.length -b.length;
}
    @Override
    public ChunkPosition put(byte[] key, ChunkPosition position) {
        lock.writeLock().lock();
        try {
            return tree.put(key, position);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public ChunkPosition get(byte[] key) {
        lock.readLock().lock();
        try {
            return tree.get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public ChunkPosition delete(byte[] key) {
        lock.writeLock().lock();
        try {
            return tree.remove(key);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public int size() {
        lock.readLock().lock();
        try {
            return tree.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void ascend(Indexer.HandleFn handleFn) throws Exception {
        lock.readLock().lock();
        try {
            for (Map.Entry<byte[], ChunkPosition> entry : tree.entrySet()) {
                if (!handleFn.handle(entry.getKey(), entry.getValue())) {
                    break;
                }
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void ascendRange(byte[] startKey, byte[] endKey, Indexer.HandleFn handleFn) throws Exception {
        lock.readLock().lock();
        try {
            for (Map.Entry<byte[], ChunkPosition> entry : tree.subMap(startKey, true, endKey, true).entrySet()) {
                if (!handleFn.handle(entry.getKey(), entry.getValue())) {
                    break;
                }
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void ascendGreaterOrEqual(byte[] key, Indexer.HandleFn handleFn) throws Exception {
        lock.readLock().lock();
        try {
            for (Map.Entry<byte[], ChunkPosition> entry : tree.tailMap(key, true).entrySet()) {
                if (!handleFn.handle(entry.getKey(), entry.getValue())) {
                    break;
                }
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void descend(Indexer.HandleFn handleFn) throws Exception {
        lock.readLock().lock();
        try {
            for (Map.Entry<byte[], ChunkPosition> entry : tree.descendingMap().entrySet()) {
                if (!handleFn.handle(entry.getKey(), entry.getValue())) {
                    break;
                }
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void descendRange(byte[] startKey, byte[] endKey, Indexer.HandleFn handleFn) throws Exception {
        lock.readLock().lock();
        try {
            for (Map.Entry<byte[], ChunkPosition> entry : tree.subMap(startKey, true, endKey, true).descendingMap().entrySet()) {
                if (!handleFn.handle(entry.getKey(), entry.getValue())) {
                    break;
                }
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void descendLessOrEqual(byte[] key, Indexer.HandleFn handleFn) throws Exception {
        lock.readLock().lock();
        try {
            for (Map.Entry<byte[], ChunkPosition> entry : tree.headMap(key, true).descendingMap().entrySet()) {
                if (!handleFn.handle(entry.getKey(), entry.getValue())) {
                    break;
                }
            }
        } finally {
            lock.readLock().unlock();
        }
    }



    // 定义接口以便于处理方法
    @FunctionalInterface
    public interface HandleFn {
        boolean handle(byte[] key, ChunkPosition position) throws Exception;
    }
}
