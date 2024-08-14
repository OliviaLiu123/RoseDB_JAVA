package main.java.com.rosedb.utils;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

//在 Java 中，可以使用 java.nio.charset.StandardCharsets 和 java.security.MessageDigest 来生成字节数组或字符串的哈希值
public class HashUtils {

    // 计算字节数组的哈希值
    public static long memHash(byte[] data) {
        return hash(data);
    }
    // 计算字符串的哈希值
    public static long memHashString(String str) {
        return hash(str.getBytes(StandardCharsets.UTF_8));
    }

    // 实际的哈希计算函数
    private static long hash(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256"); // 使用SHA-256作为哈希函数
            byte[] hashBytes = digest.digest(data);
            return bytesToLong(hashBytes); // 将生成的字节数组转换为long类型的哈希值
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    // 将字节数组转换为long
    private static long bytesToLong(byte[] bytes) {
        long result = 0;
        for (int i = 0; i < Long.BYTES && i < bytes.length; i++) {
            result <<= 8;
            result |= (bytes[i] & 0xFF);
        }
        return result;
    }



}
