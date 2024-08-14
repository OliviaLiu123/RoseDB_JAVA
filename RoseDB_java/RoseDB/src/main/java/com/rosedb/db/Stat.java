package main.java.com.rosedb.db;

public class Stat {
    private int keysNum;
    private long diskSize;

    public Stat(int keysNum, long diskSize) {
        this.keysNum = keysNum;
        this.diskSize = diskSize;
    }

    public int getKeysNum() {
        return keysNum;
    }

    public long getDiskSize() {
        return diskSize;
    }
}
