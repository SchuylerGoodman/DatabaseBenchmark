package dbtesting;

import java.sql.*;

/**
 *
 * @author thomas
 */
public class PostgresTest extends SQLTest {
	
	public PostgresTest() {
		super("jdbc:postgresql://localhost/test");
	}
}