package dbtesting;

import java.sql.*;

/**
 *
 * @author thomas
 */
public class MySQLTest extends SQLTest {

		public MySQLTest() {
			super("jdbc:mysql://localhost/test");
            this.type = "mediumblob";
			this.postCreate = " ENGINE=InnoDB";

			try {
				Class.forName("com.mysql.jdbc.Driver");
			} catch(Exception e) {
				e.printStackTrace();
			}			
		}
}