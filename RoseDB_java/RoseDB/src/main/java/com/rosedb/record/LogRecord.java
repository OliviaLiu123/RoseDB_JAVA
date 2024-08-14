package main.java.com.rosedb.record;

import org.apache.commons.lang3.tuple.Pair;
import main.java.wal.segment.ChunkPosition;
import org.apache.commons.lang3.tuple.ImmutablePair;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class LogRecord {
    private byte[] key;
    private byte[] value;
    private  byte type;
    private long batchId;
    private long expire;

    public LogRecord(byte[] key, byte[] value, byte type, long batchId, long expire) {
        this.key = key;
        this.value = value;
        this.type = type;
        this.batchId = batchId;
        this.expire = expire;
    }




    public boolean isExpired(long now) {
        return this.expire > 0 && this.expire <= now;
    }

    public  byte[] encodeLogRecord(byte[] header, ByteBuffer buf) {

        // 初始化 header

        header[0] = this.type;  // 使用直接访问字段
        int index = 1;

        // batch id
        index += putUvarint(header, index, this.batchId);  // 直接使用字段
        // key size
        index += putVarint(header, index, this.key.length);  // 直接使用字段
        // value size
        index += putVarint(header, index, this.value.length);  // 直接使用字段
        // expire
        index += putVarint(header, index, this.expire);  // 直接使用字段

        // 将header写入buf
        buf.put(header, 0, index);
        // 将key写入buf
        buf.put(this.key);  // 直接使用字段
        // 将value写入buf
        buf.put(this.value);  // 直接使用字段

        byte[] result = new byte[buf.position()];
        buf.flip();
        buf.get(result);
        return result;
    }





    public static LogRecord decodeLogRecord(byte[] buf) {
        if (buf == null) {
            throw new IllegalArgumentException("Buffer cannot be null");
        }
        if (buf.length < 1) {
            throw new IllegalArgumentException("Buffer length is too short");
        }

        int index = 1;
        byte type = buf[0];

        // batch id
        long batchId = getUvarint(buf, index);
        index += getUvarintSize(batchId);

        // key size
        int keySize = (int) getVarint(buf, index);
        index += getVarintSize(keySize);

        // value size
        int valueSize = (int) getVarint(buf, index);
        index += getVarintSize(valueSize);

        // expire
        long expire = getVarint(buf, index);
        index += getVarintSize(expire);

        // key
        byte[] key = new byte[keySize];
        System.arraycopy(buf, index, key, 0, keySize);
        index += keySize;

        // value
        byte[] value = new byte[valueSize];
        System.arraycopy(buf, index, value, 0, valueSize);

        return new LogRecord(key, value, type, batchId, expire);
    }

    private static long getUvarint(byte[] buf, int index) {
        long value = 0;
        int shift = 0;
        for (int i = index; i < buf.length; i++) {
            byte b = buf[i];
            if ((b & 0x80) == 0) {
                value |= ((long) b) << shift;
                return value;
            }
            value |= ((long) (b & 0x7F)) << shift;
            shift += 7;
        }
        return value;
    }

    private static long getVarint(byte[] buf, int index) {
        long value = getUvarint(buf, index);
        return (value >>> 1) ^ -(value & 1);
    }

    private static int getUvarintSize(long value) {
        int size = 0;
        while (value >= 0x80) {
            value >>= 7;
            size++;
        }
        return size + 1;
    }
    public static Pair<byte[], ChunkPosition> decodeHintRecord(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf);
        long segmentId = buffer.getLong();
        int blockNumber = buffer.getInt();
        long chunkOffset = buffer.getLong();
        int chunkSize = buffer.getInt();

        byte[] key = new byte[buf.length - buffer.position()];
        buffer.get(key);

        ChunkPosition position = new ChunkPosition((int) segmentId, blockNumber, chunkOffset, chunkSize);
        return new ImmutablePair<>(key, position);
    }



    public static byte[] encodeHintRecord(byte[] key, ChunkPosition pos) {
        ByteBuffer buf = ByteBuffer.allocate(25);  // 25 bytes for position data
        buf.putLong(pos.getSegmentId());
        buf.putInt(pos.getBlockNumber());
        buf.putLong(pos.getChunkOffset());
        buf.putInt(pos.getChunkSize());

        byte[] result = new byte[buf.position() + key.length];
        System.arraycopy(buf.array(), 0, result, 0, buf.position());
        System.arraycopy(key, 0, result, buf.position(), key.length);

        return result;
    }


    public static byte[] encodeMergeFinRecord(long segmentId) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.order(ByteOrder.LITTLE_ENDIAN).putInt((int) segmentId);
        return buffer.array();
    }




    private static int getVarintSize(long value) {
        return getUvarintSize((value << 1) ^ (value >> 63));
    }


    private static int putUvarint(byte[] buf, int index, long value) {
        int i = 0;
        while (value >= 0x80) {
            buf[index + i++] = (byte) (value | 0x80);
            value >>= 7;
        }
        buf[index + i++] = (byte) value;
        return i;
    }

    private static int putVarint(byte[] buf, int index, long value) {
        long v = (value << 1) ^ (value >> 63);
        return putUvarint(buf, index, v);
    }






    // Getters and setters
    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public long getBatchId() {
        return batchId;
    }

    public void setBatchId(long batchId) {
        this.batchId = batchId;
    }

    public long getExpire() {
        return expire;
    }

    public void setExpire(long expire) {
        this.expire = expire;
    }
}
