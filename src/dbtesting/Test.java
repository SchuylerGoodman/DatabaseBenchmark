package dbtesting;

/**
 * Created by schuyler on 3/21/14.
 */
public abstract class Test {

    public enum Type {mysql, sqlite, postgresql, mongo, redis, cassandra}

    private long then = 0;

    public abstract void init() throws Exception;

    public abstract long createCollection(String collectionName) throws Exception;
    public abstract long createDocument(String collectionName, int key, byte[] data0, byte[] data1, byte[] data2) throws Exception;

    public abstract long readDocument(String collectionName, int key) throws Exception;
    public abstract long readValue(String collectionName, int key, int value) throws Exception;

    public abstract long updateDocument(String collectionName, int key, byte[] data0, byte[] data1, byte[] data2) throws Exception;
    public abstract long updateValue(String collectionName, int key, byte[] data) throws Exception;

    public abstract long deleteDocument(String collectionName, int key) throws Exception;
    public abstract long deleteCollection(String collectionName) throws Exception;

    public abstract void cleanup() throws Exception;

    protected void tick() {
        this.then = System.nanoTime();
    }

    protected long tock() {

        long now = System.nanoTime();
        return now - then;
    }

}