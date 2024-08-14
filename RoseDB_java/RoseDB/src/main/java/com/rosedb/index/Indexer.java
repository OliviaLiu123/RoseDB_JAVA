package main.java.com.rosedb.index;

import main.java.wal.segment.ChunkPosition;

//Indexer 接口用于在内存中索引键和位置。
// 它用于存储键及其在WAL中的位置。
// 数据库打开时，索引将重新构建。
// 你可以通过实现这个接口来创建你自己的索引器。
public interface Indexer {
    // 将键和值放入索引中。
    ChunkPosition put(byte[] key, ChunkPosition position);
    // 获取键在索引中的位置。
    ChunkPosition get(byte[] key);

    // 删除键的索引。
    ChunkPosition delete(byte[] key);
    // 返回索引中的键数。
    int size();

    //以升序遍历项目，并为每一个新项目调用处理函数，初稿处理函数返回，false,则停止遍历

    void ascend(HandleFn handleFn) throws Exception;

    // 在[startKey, endKey]范围内以升序遍历，调用 handleFn。
    // 如果 handleFn 返回 false，则停止遍历。
    void ascendRange(byte[] startKey, byte[] endKey, HandleFn handleFn) throws Exception;

    // 从 >= 给定键开始以升序遍历，调用 handleFn。
    // 如果 handleFn 返回 false，则停止遍历。
    void ascendGreaterOrEqual(byte[] key, HandleFn handleFn) throws Exception;

    // 以降序遍历项目，并为每个项目调用处理函数。
    // 如果处理函数返回 false，则停止遍历。
    void descend(HandleFn handleFn) throws Exception;

    // 在[startKey, endKey]范围内以降序遍历，调用 handleFn。
    // 如果 handleFn 返回 false，则停止遍历。
    void descendRange(byte[] startKey, byte[] endKey, HandleFn handleFn) throws Exception;

    // 从 <= 给定键开始以降序遍历，调用 handleFn。
    // 如果 handleFn 返回 false，则停止遍历。
    void descendLessOrEqual(byte[] key, HandleFn handleFn) throws Exception;

    @FunctionalInterface
    interface HandleFn {
        boolean handle(byte[] key, ChunkPosition position) throws Exception;
    }


}
