package main.java.com.rosedb.watch;

public class Event {
    private WatchActionType action;
    private byte[] key;
    private byte[] value;
    private long batchId;

    // Constructor
    public Event(WatchActionType action, byte[] key, byte[] value, long batchId) {
        this.action = action;
        this.key = key;
        this.value = value;
        this.batchId = batchId;
    }

    // Getters and Setters
    public WatchActionType getAction() {
        return action;
    }

    public void setAction(WatchActionType action) {
        this.action = action;
    }

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

    public long getBatchId() {
        return batchId;
    }

    public void setBatchId(long batchId) {
        this.batchId = batchId;
    }
}
