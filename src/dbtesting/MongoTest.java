package dbtesting;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class MongoTest implements Test
{
	MongoClient mongoClient; 
	DB db; 
	DBCollection collection; 
	
	@Override
	public void init() throws Exception 
	{
		mongoClient = new MongoClient(); 
		db = mongoClient.getDB("monTestFinal"); 
	}

	@Override
	public void createCollection(String collectionName) throws Exception 
	{ 
		
		collection =  db.createCollection(collectionName, null);
		
	}

	@Override
	public void createDocument(String collectionName, int key, byte[] data0, byte[] data1, byte[] data2) throws Exception {
		BasicDBObject doc = new BasicDBObject("key", key).append("a", data0).append("b", data1).append("c", data2); 
		collection.insert(doc); 
		
	}

	@Override
	public void readDocument(String collectionName, int key) throws Exception {
		BasicDBObject query = new BasicDBObject("key", key);
		DBCursor cursor = collection.find(query);
		DBObject o = cursor.next();
		
		cursor.close();

	}

	@Override
	public void readValue(String collectionName, int key, int value)
			throws Exception {
		BasicDBObject query = new BasicDBObject("key", key);
		BasicDBObject project = new BasicDBObject("a", value);
		DBCursor cursor = collection.find(query,project);
		
		DBObject o = cursor.next();
	
		cursor.close();
		
	}

	@Override
	public void updateDocument(String collectionName, int key, byte[] data0,
			byte[] data1, byte[] data2) throws Exception {
		BasicDBObject query = new BasicDBObject("key", key);
		BasicDBObject update = new BasicDBObject("key", key).append("a", data0).append("b", data1).append("c", data2); 
		
		collection.findAndModify(query, update); 
		
		
	}

	@Override
	public void updateValue(String collectionName, int key, byte[] data)
			throws Exception {
		BasicDBObject query = new BasicDBObject("key", key);
		BasicDBObject update = new BasicDBObject("key", key).append("a", data); 
		
		collection.findAndModify(query, update); 
		
	}

	@Override
	public void deleteDocument(String collectionName, int key) throws Exception {
		BasicDBObject query = new BasicDBObject("key", key);
		collection.remove(query); 
	}

	@Override
	public void deleteCollection(String collectionName) throws Exception {
		collection.drop();
		
	}

	@Override
	public void cleanup() throws Exception {
		db.dropDatabase();
		mongoClient.close();
		
	}

}
