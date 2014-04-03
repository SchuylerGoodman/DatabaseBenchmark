DatabaseBenchmark
=================
To compile:
javac -cp src:lib/postgresql-9.3-1101.jdbc41.jar:lib/sqlite-jdbc-3.7.2.jar:lib/jedis-2.1.0.jar:lib/mongo-2.10.1.jar:lib/mysql-connector-java-5.1.30-bin.jar src/dbtesting/*.java

To run:
java -cp src:lib/postgresql-9.3-1101.jdbc41.jar:lib/sqlite-jdbc-3.7.2.jar:lib/jedis-2.1.0.jar:lib/mongo-2.10.1.jar:lib/mysql-connector-java-5.1.30-bin.jar dbtesting.App