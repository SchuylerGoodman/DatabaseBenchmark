/**
 * Created by schuyler on 3/21/14.
 */
public interface Test {

    public void init();

    public void createCollection(int collectionName);
    public void createDocument(int collectionName, int key, byte[] data0, byte[] data1, byte[] data2);

    public void readDocument(int collectionName, int key);
    public void readValue(int collectionName, int key, int value);

    public void updateDocument(int collectionName, int key, byte[] data0, byte[] data1, byte[] data2);
    public void updateValue(int collectionName, int key, byte[] data);

    public void deleteDocument(int collectionName, int key);
    public void deleteCollection(int collectionName);

    public void cleanup();

}
