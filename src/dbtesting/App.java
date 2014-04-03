package dbtesting;

import java.util.*;

/**
 * Hello world!
 *
 */
public class App
{
	static int SMALL_COUNT = 1000;
	static int LARGE_COUNT = 10;

	static long lastTime;

	static HashMap<String, Double> times = new HashMap<String, Double>();

	public static void mark(String name) {
		long end = System.nanoTime();

		double elapsed = (end - lastTime) / 1e9;
		times.put(name, elapsed);
		System.out.println(name + "," + elapsed);

		lastTime = end;
	}

    public static void main( String[] args )
    {
    	Class[] tests = new Class[]{MySQLTest.class, SQLiteTest.class, PostgresTest.class, MongoTest.class, RedisTest.class};

		try {

			DataGenerator gen = new StaticGenerator();

			//Test t = new MySQLTest();
			Test t = (Test)tests[2].newInstance();
			t.init();
			System.out.println("Creating Collection");
			t.createCollection("a");

			lastTime = System.nanoTime();
			for (int i = 0; i < SMALL_COUNT; i++) {
				t.createDocument("a", i, gen.getSmallData(), gen.getSmallData(), gen.getSmallData());
			}
			mark("create");
			for (int i = 0; i < SMALL_COUNT; i++) {
				t.updateDocument("a", i, gen.getSmallData(), gen.getSmallData(), gen.getSmallData());
			}
			mark("updateD");
			for (int i = 0; i < SMALL_COUNT; i++) {
				t.readDocument("a", i);
			}
			mark("readD");
			for (int i = 0; i < SMALL_COUNT; i++) {
				t.readValue("a", i, 1);
			}
			mark("readV");
			for (int i = 0; i < SMALL_COUNT; i++) {
				t.updateValue("a", i, gen.getSmallData());
			}
			mark("updateV");
			for (int i = 0; i < SMALL_COUNT; i++) {
				t.deleteDocument("a", i);
			}
			mark("delete");

			for(String key : times.keySet()) {
				System.out.println(key + "," + times.get(key));
			}


			for (int i = 0; i < LARGE_COUNT; i++) {
				t.createDocument("a", i, gen.getLargeData(), gen.getLargeData(), gen.getLargeData());
			}
			for (int i = 0; i < LARGE_COUNT; i++) {
				t.readDocument("a", i);
			}
			for (int i = 0; i < LARGE_COUNT; i++) {
				t.readValue("a", i, 1);
			}
			for (int i = 0; i < LARGE_COUNT; i++) {
				t.updateDocument("a", i, gen.getLargeData(), gen.getLargeData(), gen.getLargeData());
			}
			for (int i = 0; i < LARGE_COUNT; i++) {
				t.updateValue("a", i, gen.getLargeData());
			}
			for (int i = 0; i < LARGE_COUNT; i++) {
				t.deleteDocument("a", i);
			}

			//System.in.read();
			System.out.println("Deleting Collection");
			t.deleteCollection("a");

            t.cleanup();

		} catch (Exception e) {
			e.printStackTrace();
		}
    }
}
