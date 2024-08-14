package main.java.wal.writeAheadLog;


import org.apache.commons.lang3.tuple.Pair;
import main.java.wal.segment.ChunkPosition;
import main.java.wal.segment.SegmentReader;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;

//这个类代表了WAL的读取器，用于从WAL中读取数据。
public class Reader {
    private List<SegmentReader> segmentReaders;//用于存储所有段的SegmentReader
    private int currentReaderIndex;// 当前读取的SegmentReader 的索引

    // 构造函数


    public Reader(List<SegmentReader> segmentReaders) {
        this.segmentReaders = segmentReaders;
        this.currentReaderIndex =0;// 初始化为第一个 SegmentReader
    }

    // 获取下一个数据块及其位置，如果没有数据，则抛出 EOFException
//    public Pair<byte[], ChunkPosition> next() throws IOException {
//        if (currentReaderIndex >= segmentReaders.size()) {
//            throw new EOFException("No more data in WAL");
//        }
//
//        try {
//            SegmentReader currentReader = segmentReaders.get(currentReaderIndex);
//            return Pair.of(currentReader.next(), currentReader.getCurrentChunkPosition());
//        } catch (EOFException e) {
//            currentReaderIndex++;
//            return next();  // 递归调用以继续读取下一个 SegmentReader
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }





    public Pair<byte[], ChunkPosition> next() throws Exception {
        while (currentReaderIndex < segmentReaders.size()) {
            SegmentReader currentReader = segmentReaders.get(currentReaderIndex);
            byte[] chunk = currentReader.next();

            if (chunk == null) {
                System.out.println("Chunk is null, moving to the next segment reader.");
                currentReaderIndex++;
                continue;  // 继续读取下一个 SegmentReader
            }

            return Pair.of(chunk, currentReader.getCurrentChunkPosition());
        }

        // 如果没有更多的数据
        System.out.println("No more data in WAL.");
        return null;
    }


    // 跳过当前段文件
    public void skipCurrentSegment() {
        currentReaderIndex++;
    }

    // 获取当前段文件的 ID
    public int currentSegmentId() {
        return segmentReaders.get(currentReaderIndex).getSegment().getSegmentId();
    }

    // 获取当前块数据的位置
    public ChunkPosition currentChunkPosition() {
        return segmentReaders.get(currentReaderIndex).getCurrentChunkPosition();
    }


// 获取当前segmentid
public int getCurrentSegmentId() {
    return segmentReaders.get(currentReaderIndex).getSegment().getSegmentId();
}




//getter and setter


    public List<SegmentReader> getSegmentReaders() {
        return segmentReaders;
    }

    public void setSegmentReader(List<SegmentReader> segmentReader) {
        this.segmentReaders = segmentReaders;
    }

    public int getCurrentReaderIndex() {
        return currentReaderIndex;
    }

    public void setCurrentReaderIndex(int currentReaderIndex) {
        this.currentReaderIndex = currentReaderIndex;
    }
}
