package dbtesting;

import com.datastax.driver.core.*;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by schuyler on 3/28/14.
 */
public class CassandraTest implements Test {

    private CassandraClient client;

    @Override
    public void init() throws Exception {
        client = new CassandraClient();
        client.connect("127.0.0.1");
    }

    @Override
    public void createCollection(String collectionName) throws Exception {
        getSession().execute("CREATE TABLE " + client.keyspace + "." + collectionName +
                                " (id int PRIMARY KEY, a blob, b blob, c blob);");
    }

    @Override
    public void createDocument(String collectionName, int key, byte[] data0, byte[] data1, byte[] data2) throws Exception {
        ByteBuffer bb0 = ByteBuffer.wrap(data0);
        ByteBuffer bb1 = ByteBuffer.wrap(data1);
        ByteBuffer bb2 = ByteBuffer.wrap(data2);
        PreparedStatement st = getSession().prepare("INSERT INTO " + client.keyspace + "." + collectionName +
                                                        " (id, a, b, c) VALUES (?, ?, ?, ?)");
        BoundStatement bs = new BoundStatement(st);
        getSession().execute(bs.bind(key, bb0, bb1, bb2));
    }

    @Override
    public void readDocument(String collectionName, int key) throws Exception {
        ResultSet rs = getSession().execute("SELECT * FROM " + client.keyspace + "." + collectionName + " WHERE id=" + key);
        List<Row> rows = rs.all();
        if (rows.size() != 1) {
            System.out.println("Error in CassandraTest.ReadDocument -- Should only have one row in read, found " + rows.size());
        }
//        for (Row row : rows) {
////            row.toString();
//        }
    }

    @Override
    public void readValue(String collectionName, int key, int value) throws Exception {
        ResultSet rs = getSession().execute("SELECT a FROM " + client.keyspace + "." + collectionName + " WHERE id = " + key);
        Row row = rs.one();
//        row.toString();
    }

    @Override
    public void updateDocument(String collectionName, int key, byte[] data0, byte[] data1, byte[] data2) throws Exception {
        ByteBuffer bb0 = ByteBuffer.wrap(data0);
        ByteBuffer bb1 = ByteBuffer.wrap(data1);
        ByteBuffer bb2 = ByteBuffer.wrap(data2);
        PreparedStatement st = getSession().prepare("UPDATE " + client.keyspace + "." + collectionName + " SET a = ?, b = ?, c = ? WHERE id = " + key);
        BoundStatement bs = new BoundStatement(st);
        getSession().execute(bs.bind(bb0, bb1, bb2));
    }

    @Override
    public void updateValue(String collectionName, int key, byte[] data) throws Exception {
        ByteBuffer bb = ByteBuffer.wrap(data);
        PreparedStatement st = getSession().prepare("UPDATE " + client.keyspace + "." + collectionName + " SET a = ? WHERE id = " + key);
        BoundStatement bs = new BoundStatement(st);
        getSession().execute(bs.bind(bb));
    }

    @Override
    public void deleteDocument(String collectionName, int key) throws Exception {
        getSession().execute("DELETE FROM " + client.keyspace + "." + collectionName + " WHERE id = " + key);
    }

    @Override
    public void deleteCollection(String collectionName) throws Exception {
        getSession().execute("DROP TABLE " + client.keyspace + "." + collectionName);
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

