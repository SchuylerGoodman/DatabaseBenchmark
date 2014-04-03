package dbtesting;

import java.sql.*;

/**
 *
 * @author thomas
 */
public class SQLiteTest extends SQLTest {

	public SQLiteTest() {
		super("jdbc:sqlite:test.db");
		try {
			Class.forName("org.sqlite.JDBC");
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}