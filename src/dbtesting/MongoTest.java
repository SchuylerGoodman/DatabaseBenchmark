package dbtesting;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class MongoTest extends Test
{
	MongoClient mongoClient; 
	DB db; 
	DBCollection collection; 
	
	@Override
	public void init() throws Exception 
	{
		mongoClient = new MongoClient(); 
		db = mongoClient.getDB("test");
	}

	@Override
	public long createCollection(String collectionName) throws Exception
	{ 
		tick();
		collection =  db.createCollection(collectionName, null);
		return tock();
	}

	@Override
	public long createDocument(String collectionName, int key, byte[] data0, byte[] data1, byte[] data2) throws Exception {
		BasicDBObject doc = new BasicDBObject("key", key).append("a", data0).append("b", data1).append("c", data2);
        tick();
		collection.insert(doc); 
		return tock();
	}

	@Override
	public long readDocument(String collectionName, int key) throws Exception {

		BasicDBObject query = new BasicDBObject("key", key);

        tick();
		DBCursor cursor = collection.find(query);
		cursor.close();
        return tock();

	}

	@Override
	public long readValue(String collectionName, int key, int value)
			throws Exception {
		BasicDBObject query = new BasicDBObject("key", key);
		BasicDBObject project = new BasicDBObject("a", value);

        tick();
		DBCursor cursor = collection.find(query,project);
		cursor.close();
        return tock();
	}

	@Override
	public long updateDocument(String collectionName, int key, byte[] data0,
			byte[] data1, byte[] data2) throws Exception {
		BasicDBObject query = new BasicDBObject("key", key);
		BasicDBObject update = new BasicDBObject("key", key).append("a", data0).append("b", data1).append("c", data2); 

        tick();
		collection.findAndModify(query, update); 
		return tock();
	}

	@Override
	public long updateValue(String collectionName, int key, byte[] data)
			throws Exception {
		BasicDBObject query = new BasicDBObject("key", key);
		BasicDBObject update = new BasicDBObject("key", key).append("a", data); 

        tick();
		collection.findAndModify(query, update); 
		return tock();
	}

	@Override
	public long deleteDocument(String collectionName, int key) throws Exception {
		BasicDBObject query = new BasicDBObject("key", key);
        tick();
		collection.remove(query);
        return tock();
	}

	@Override
	public long deleteCollection(String collectionName) throws Exception {
        tick();
		collection.drop();
		return tock();
	}

	@Override
	public void cleanup() throws Exception {
		db.dropDatabase();
		mongoClient.close();
		
	}

}
