package dbtesting;

/**
 * Created by schuyler on 3/21/14.
 */
public interface Test {

    public void init() throws Exception;

    public void createCollection(String collectionName) throws Exception;
    public void createDocument(String collectionName, int key, byte[] data0, byte[] data1, byte[] data2) throws Exception;

    public void readDocument(String collectionName, int key) throws Exception;
    public void readValue(String collectionName, int key, int value) throws Exception;

    public void updateDocument(String collectionName, int key, byte[] data0, byte[] data1, byte[] data2) throws Exception;
    public void updateValue(String collectionName, int key, byte[] data) throws Exception;

    public void deleteDocument(String collectionName, int key) throws Exception;
    public void deleteCollection(String collectionName) throws Exception;

    public void cleanup() throws Exception;

}