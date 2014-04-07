package dbtesting;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.*;

/**
 * Hello world!
 *
 */
public class Benchmarker
{
	static int SMALL_COUNT = 1000;
	static int LARGE_COUNT = 10;
    private static final int BENCHMARK_COUNT = 10;

    static long lastTime;

    static HashMap<String, Double> times = new HashMap<String, Double>();
    static EnumMap<Test.Type, Class> testTypes = new EnumMap<Test.Type, Class>(Test.Type.class);

    private static Logger log;
    static {

        initLog();

        // Initialize test types in type map
        testTypes.put(Test.Type.mysql, MySQLTest.class);
        testTypes.put(Test.Type.sqlite, SQLiteTest.class);
        testTypes.put(Test.Type.postgresql, PostgresTest.class);
        testTypes.put(Test.Type.mongo, MongoTest.class);
        testTypes.put(Test.Type.redis, RedisTest.class);
        testTypes.put(Test.Type.cassandra, CassandraTest.class);
    }


    private Benchmark[] benchmarks = new Benchmark[BENCHMARK_COUNT];

    public Benchmarker() {
    }

	public static void mark(String name) {
		long end = System.nanoTime();

		double elapsed = (end - lastTime) / 1e9;
		times.put(name, elapsed);
		System.out.println(name + "," + elapsed);

		lastTime = end;
	}

    public static void main( String[] args )
    {
        lastTime = System.nanoTime();
        int a = 97;
        Benchmarker benchmarker = new Benchmarker();
        DataGenerator gen = new StaticGenerator();

        for (Test.Type type : testTypes.keySet()) {

            try {

                log.fine("Starting " + type.name() + " benchmarks...");
                Test t = (Test)testTypes.get(type).newInstance();
                t.init();
                for (int count = 0; count < BENCHMARK_COUNT; ++count) {

                    log.fine(type.name() + " - Benchmark #" + count);
                    Benchmark benchmark = benchmarker.getBenchmark(count);
                    String collection = String.valueOf((char) (a + count));

                    log.fine(type.name() + " - Creating Collection " + collection);
                    benchmark.addTime(type, 0, 0, t.createCollection(collection));

                    // SMALL BENCHMARK TESTS
                    log.finer(type.name() + " - Creating small documents");
                    for (int i = 0; i < SMALL_COUNT; i++) {
                        log.finest(type.name() + " - Creating small document " + i);
                        benchmark.addTime(type, 1, i,
                                t.createDocument(collection, i, gen.getSmallData(),
                                        gen.getSmallData(), gen.getSmallData()));
                    }

                    log.finer(type.name() + " - Updating small documents");
                    for (int i = 0; i < SMALL_COUNT; i++) {
                        log.finest(type.name() + " - Updating small document " + i);
                        benchmark.addTime(type, 2, i,
                                t.updateDocument(collection, i, gen.getSmallData(),
                                        gen.getSmallData(), gen.getSmallData()));
                    }

                    log.finer(type.name() + " - Reading small documents");
                    for (int i = 0; i < SMALL_COUNT; i++) {
                        log.finest(type.name() + " - Reading small document " + i);
                        benchmark.addTime(type, 3, i, t.readDocument(collection, i));
                    }

                    log.finer(type.name() + " - Reading small values");
                    for (int i = 0; i < SMALL_COUNT; i++) {
                        log.finest(type.name() + " - Reading small value " + i);
                        benchmark.addTime(type, 4, i, t.readValue(collection, i, 1));
                    }

                    log.finer(type.name() + " - Updating small values");
                    for (int i = 0; i < SMALL_COUNT; i++) {
                        log.finest(type.name() + " - Updating small value " + i);
                        benchmark.addTime(type, 5, i, t.updateValue(collection, i, gen.getSmallData()));
                    }

                    log.finer(type.name() + " - Deleting small documents");
                    for (int i = 0; i < SMALL_COUNT; i++) {
                        log.finest(type.name() + " - Deleting small document " + i);
                        benchmark.addTime(type, 6, i, t.deleteDocument(collection, i));
                    }


                    // LARGE BENCHMARK TESTS
                    log.finer(type.name() + " - Creating large documents");
                    for (int i = 0; i < LARGE_COUNT; i++) {
                        log.finest(type.name() + " - Creating large document " + i);
                        benchmark.addTime(type, 7, i,
                                          t.createDocument(collection, i, gen.getLargeData(),
                                                           gen.getLargeData(), gen.getLargeData()));
                    }

                    log.finer(type.name() + " - Updating large documents");
                    for (int i = 0; i < LARGE_COUNT; i++) {
                        log.finest(type.name() + " - Updating large document " + i);
                        benchmark.addTime(type, 8, i,
                                          t.updateDocument(collection, i, gen.getLargeData(),
                                                           gen.getLargeData(), gen.getLargeData()));
                    }

                    log.finer(type.name() + " - Reading large documents");
                    for (int i = 0; i < LARGE_COUNT; i++) {
                        log.finest(type.name() + " - Reading large document " + i);
                        benchmark.addTime(type, 9, i, t.readDocument(collection, i));
                    }

                    log.finer(type.name() + " - Reading large values");
                    for (int i = 0; i < LARGE_COUNT; i++) {
                        log.finest(type.name() + " - Reading large value: " + i);
                        benchmark.addTime(type, 10, i, t.readValue(collection, i, 1));
                    }

                    log.finer(type.name() + " - Updating large values");
                    for (int i = 0; i < LARGE_COUNT; i++) {
                        log.finest(type.name() + " - Updating large value: " + i);
                        benchmark.addTime(type, 11, i, t.updateValue(collection, i, gen.getLargeData()));
                    }

                    log.finer(type.name() + " - Deleting large documents");
                    for (int i = 0; i < LARGE_COUNT; i++) {
                        log.finest(type.name() + " - Deleting large document " + i);
                        benchmark.addTime(type, 12, i, t.deleteDocument(collection, i));
                    }

                    log.fine(type.name() + " - Deleting Collection " + collection);
                    benchmark.addTime(type, 13, 0, t.deleteCollection(collection));

                }

                t.cleanup();

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        long endTime = System.nanoTime();
        long endTimeSeconds = TimeUnit.SECONDS.convert(endTime - lastTime, TimeUnit.NANOSECONDS);
        log.fine("Finished all tests in " + endTimeSeconds + " seconds");

        log.fine("Saving data to file...");
        for (int count = 0; count < BENCHMARK_COUNT; ++count) {
            Benchmark benchmark = benchmarker.getBenchmark(count);
            benchmark.saveToCSV(count);
        }
    }

    public Benchmark getBenchmark(int count) {
        if (this.benchmarks[count] == null) {
            this.benchmarks[count] = new Benchmark();
        }
        return this.benchmarks[count];
    }

    private class Benchmark {

        private static final int TEST_NUMBER = 14;

        private long[][] mysql = new long[TEST_NUMBER][SMALL_COUNT];
        private long[][] sqlite = new long[TEST_NUMBER][SMALL_COUNT];
        private long[][] postgresql = new long[TEST_NUMBER][SMALL_COUNT];
        private long[][] mongodb = new long[TEST_NUMBER][SMALL_COUNT];
        private long[][] redis = new long[TEST_NUMBER][SMALL_COUNT];
        private long[][] cassandra = new long[TEST_NUMBER][SMALL_COUNT];

        public Benchmark() {}

        public void addTime(Test.Type type, int testNum, int iteration, long time) throws Exception {

            switch (type) {
                case mysql:
                    addMySQLTime(testNum, iteration, time); break;
                case sqlite:
                    addSQLiteTime(testNum, iteration, time); break;
                case postgresql:
                    addPostgresTime(testNum, iteration, time); break;
                case mongo:
                    addMongoTime(testNum, iteration, time); break;
                case redis:
                    addRedisTime(testNum, iteration, time); break;
                case cassandra:
                    addCassandraTime(testNum, iteration, time); break;
                default:
                    throw new Exception("No benchmark for given type");
            }
        }

        public void saveToCSV(int index) {

            log.fine("Saving Benchmark #" + index);
            saveSingleToCSV("mysql-" + index, mysql);
            saveSingleToCSV("sqlite-" + index, sqlite);
            saveSingleToCSV("postgresql-" + index, postgresql);
            saveSingleToCSV("mongodb-" + index, mongodb);
            saveSingleToCSV("redis-" + index, redis);
            saveSingleToCSV("cassandra-" + index, cassandra);
        }

        private void saveSingleToCSV(String fileName, long[][] data) {

            String fullName = "data/" + fileName + ".tab";
            log.fine("Attempting to save data to " + fullName);
            try {

                // Tab delimited file will be written to data
                FileWriter fos = new FileWriter(fullName);
                PrintWriter dos = new PrintWriter(fos);

                // Create data headers
                dos.println("Create Collection\tCreate Small Documents\tUpdate Small Documents\tRead Small Documents\tRead Small Values\tUpdate Small Values\tDelete Small Documents\tCreate Large Documents\tUpdate Large Documents\tRead Large Documents\tRead Large Values\tUpdate Large Values\tDelete Large Documents\tDelete Collection");

                // print all data to file
                for (int i = 0; i < SMALL_COUNT; ++i) {
                    for (int j = 0; j < TEST_NUMBER; ++j) {
                        dos.print(data[j][i] + "\t");
                    }
                    dos.println();
                }

                dos.close();
                fos.close();
            } catch (IOException e) {
                log.log(Level.SEVERE, "Error saving data to " + fullName, e);
            }

        }

        private void addMySQLTime(int testNum, int iteration, long time) {
            mysql[testNum][iteration] = time;
        }

        private void addSQLiteTime(int testNum, int iteration, long time) {
            sqlite[testNum][iteration] = time;
        }

        private void addPostgresTime(int testNum, int iteration, long time) {
            postgresql[testNum][iteration] = time;
        }

        private void addMongoTime(int testNum, int iteration, long time) {
            mongodb[testNum][iteration] = time;
        }

        private void addRedisTime(int testNum, int iteration, long time) {
            redis[testNum][iteration] = time;
        }

        private void addCassandraTime(int testNum, int iteration, long time) {
            cassandra[testNum][iteration] = time;
        }

    }

    public static void initLog() {

        try {
            Level logLevel = Level.FINER;

            log = Logger.getLogger("DatabaseBenchmark");
            log.setLevel(logLevel);
            log.setUseParentHandlers(false);

            Handler consoleHandler = new ConsoleHandler();
            consoleHandler.setLevel(logLevel);
            consoleHandler.setFormatter(new SimpleFormatter());
            log.addHandler(consoleHandler);

            FileHandler fileHandler = new FileHandler("benchmark.log", false);
            fileHandler.setLevel(logLevel);
            fileHandler.setFormatter(new SimpleFormatter());
            log.addHandler(fileHandler);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
