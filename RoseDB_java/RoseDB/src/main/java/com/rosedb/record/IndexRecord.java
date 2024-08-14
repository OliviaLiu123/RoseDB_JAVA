package main.java.com.rosedb.record;


import main.java.wal.segment.ChunkPosition;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class IndexRecord {
    private byte[] key;
    private byte recordType;
    private ChunkPosition position;

    public IndexRecord(byte[] key, byte recordType, ChunkPosition position) {
        this.key = key;
        this.recordType = recordType;
        this.position = position;
    }



    // Getters and setters
    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte getRecordType() {
        return recordType;
    }

    public void setRecordType(byte recordType) {
        this.recordType = recordType;
    }

    public ChunkPosition getPosition() {
        return position;
    }

    public void setPosition(ChunkPosition position) {
        this.position = position;
    }
}
