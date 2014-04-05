package dbtesting;

import com.datastax.driver.core.*;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by schuyler on 3/28/14.
 */
public class CassandraTest extends Test {

    private CassandraClient client;

    @Override
    public void init() throws Exception {
        client = new CassandraClient();
        client.connect("127.0.0.1");
    }

    @Override
    public long createCollection(String collectionName) throws Exception {

        String query = "CREATE TABLE " + client.keyspace + "." + collectionName + " (id int PRIMARY KEY, a blob, b blob, c blob);";

        tick();
        getSession().execute(query);
        return tock();
    }

    @Override
    public long createDocument(String collectionName, int key, byte[] data0, byte[] data1, byte[] data2) throws Exception {

        String query = "INSERT INTO " + client.keyspace + "." + collectionName + " (id, a, b, c) VALUES (?, ?, ?, ?)";
        ByteBuffer bb0 = ByteBuffer.wrap(data0);
        ByteBuffer bb1 = ByteBuffer.wrap(data1);
        ByteBuffer bb2 = ByteBuffer.wrap(data2);
        PreparedStatement st = getSession().prepare(query);
        BoundStatement bs = new BoundStatement(st);

        tick();
        getSession().execute(bs.bind(key, bb0, bb1, bb2));
        return tock();
    }

    @Override
    public long readDocument(String collectionName, int key) throws Exception {

        String query = "SELECT * FROM " + client.keyspace + "." + collectionName + " WHERE id=" + key;

        tick();
        ResultSet rs = getSession().execute(query);
        return tock();
    }

    @Override
    public long readValue(String collectionName, int key, int value) throws Exception {

        String query = "SELECT a FROM " + client.keyspace + "." + collectionName + " WHERE id = " + key;

        tick();
        ResultSet rs = getSession().execute(query);
        return tock();
    }

    @Override
    public long updateDocument(String collectionName, int key, byte[] data0, byte[] data1, byte[] data2) throws Exception {

        String query = "UPDATE " + client.keyspace + "." + collectionName + " SET a = ?, b = ?, c = ? WHERE id = " + key;

        ByteBuffer bb0 = ByteBuffer.wrap(data0);
        ByteBuffer bb1 = ByteBuffer.wrap(data1);
        ByteBuffer bb2 = ByteBuffer.wrap(data2);
        PreparedStatement st = getSession().prepare(query);
        BoundStatement bs = new BoundStatement(st);

        tick();
        getSession().execute(bs.bind(bb0, bb1, bb2));
        return tock();
    }

    @Override
    public long updateValue(String collectionName, int key, byte[] data) throws Exception {

        String query = "UPDATE " + client.keyspace + "." + collectionName + " SET a = ? WHERE id = " + key;

        ByteBuffer bb = ByteBuffer.wrap(data);
        PreparedStatement st = getSession().prepare(query);
        BoundStatement bs = new BoundStatement(st);

        tick();
        getSession().execute(bs.bind(bb));
        return tock();
    }

    @Override
    public long deleteDocument(String collectionName, int key) throws Exception {

        String query = "DELETE FROM " + client.keyspace + "." + collectionName + " WHERE id = " + key;

        tick();
        getSession().execute(query);
        return tock();
    }

    @Override
    public long deleteCollection(String collectionName) throws Exception {

        String query = "DROP TABLE " + client.keyspace + "." + collectionName;

        tick();
        getSession().execute(query);
        return tock();
    }

    @Override
    public void cleanup() throws Exception {
        client.close();
    }

    private Session getSession() {
        return client.getSession();
    }

}

class CassandraClient {

    private Cluster cluster;
    private Session session;

    public String keyspace = "databaseBenchmark";

    public void connect(String node) {
        cluster = cluster.builder().addContactPoint(node).build();
        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
        for (Host host : metadata.getAllHosts()) {
            System.out.printf("Datacenter: %s; Host: %s; Rack: %s \n", host.getDatacenter(), host.getAddress(), host.getRack());
        }
        session = cluster.newSession();
        session.execute("CREATE KEYSPACE " + keyspace + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
    }

    public void close() {
        session.execute("DROP KEYSPACE " + keyspace);
        session.close();
        cluster.close();
    }

    public Session getSession() {
        return session;
    }

}

