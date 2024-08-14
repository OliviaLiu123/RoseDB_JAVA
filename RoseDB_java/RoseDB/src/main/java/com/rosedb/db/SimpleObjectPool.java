package main.java.com.rosedb.db;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class SimpleObjectPool<T> {

    // 对象池，用于存储可用的对象实例
    private final BlockingQueue<T> pool;

    // 对象创建的工厂，用于创建新的对象实例
    private final ObjectFactory<T> factory;

    // 构造函数，初始化对象池和工厂
    public SimpleObjectPool(int capacity, ObjectFactory<T> factory) {
        this.pool = new LinkedBlockingQueue<>(capacity);
        this.factory = factory;
        // 初始化对象池，填充初始对象
        for (int i = 0; i < capacity; i++) {
            pool.offer(factory.createObject());
        }
    }

    // 从对象池中获取一个对象，如果池中没有可用对象，则创建一个新的对象
    public T get() {
        T obj = pool.poll(); // 从池中取出对象
        return (obj != null) ? obj : factory.createObject(); // 如果没有对象可用，则创建新的对象
    }

    // 将对象归还到对象池中，重用该对象
    public void put(T obj) {
        pool.offer(obj); // 将对象放回池中
    }

    // 获取当前对象池的大小
    public int getPoolSize() {
        return pool.size();
    }

    // 对象工厂接口，用于创建对象实例
    public interface ObjectFactory<T> {
        T createObject();
    }
}
