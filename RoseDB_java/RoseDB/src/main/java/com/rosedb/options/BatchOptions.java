package main.java.com.rosedb.options;

public class BatchOptions {
    private boolean sync;  // Sync has the same semantics as Options.Sync.
    private boolean readOnly;  // ReadOnly specifies whether the batch is read-only.

    // Constructor with all parameters
    public BatchOptions(boolean sync, boolean readOnly) {
        this.sync = sync;
        this.readOnly = readOnly;
    }
    // Default constructor with default values

    // Getters and setters
    public boolean isSync() {
        return sync;
    }

    public void setSync(boolean sync) {
        this.sync = sync;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }
}

