package main.java.com.rosedb.utils;

import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

//为测试生成格式化的键和值
public class RandKVUtils {
    private static final char[] LETTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".toCharArray();
    private static final Lock lock = new ReentrantLock();
    private static final Random rand = new Random();

    // 生成格式化的测试键，类似于rosedb-test-key-000000001
    public static String getTestKey(int i) {
        return String.format("rosedb-test-key-%09d", i);
    }

    // 生成随机值，类似于rosedb-test-value-xxxxxxx
    public static String randomValue(int n) {
        StringBuilder sb = new StringBuilder(n + 17); // 17 = "rosedb-test-value-".length()
        sb.append("rosedb-test-value-");

        lock.lock();
        try {
            for (int i = 0; i < n; i++) {
                sb.append(LETTERS[rand.nextInt(LETTERS.length)]);
            }
        } finally {
            lock.unlock();
        }

        return sb.toString();
    }

    public static void main(String[] args) {
        // 测试生成的键和值
        System.out.println(getTestKey(1));  // 输出: rosedb-test-key-000000001
        System.out.println(randomValue(10));  // 输出: 类似于 rosedb-test-value-Abc123xyz
    }
}
