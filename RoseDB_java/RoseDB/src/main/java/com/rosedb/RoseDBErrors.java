package main.java.com.rosedb;

public class RoseDBErrors {
    public static final RuntimeException ERR_KEY_IS_EMPTY =
            new RuntimeException("The key is empty");

    public static final RuntimeException ERR_KEY_NOT_FOUND =
            new RuntimeException("Key not found in database");

    public static final RuntimeException ERR_DATABASE_IS_USING =
            new RuntimeException("The database directory is used by another process");

    public static final RuntimeException ERR_READ_ONLY_BATCH =
            new RuntimeException("The batch is read-only");

    public static final RuntimeException ERR_BATCH_COMMITTED =
            new RuntimeException("The batch is committed");

    public static final RuntimeException ERR_BATCH_ROLLBACKED =
            new RuntimeException("The batch is rollbacked");

    public static final RuntimeException ERR_DB_CLOSED =
            new RuntimeException("The database is closed");

    public static final RuntimeException ERR_MERGE_RUNNING =
            new RuntimeException("The merge operation is running");

    public static final RuntimeException ERR_WATCH_DISABLED =
            new RuntimeException("The watch is disabled");
}
