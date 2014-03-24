package dbtesting;

import java.sql.*;

/**
 *
 * @author thomas
 */
public class SQLTest implements Test {

	Connection con = null;
	Statement st = null;
	ResultSet rs = null;


	//String url = "jdbc:postgresql://localhost/test";
	String url = "jdbc:sqlite:test.db";
	String user = "admin";
	String password = "";

	@Override
	public void init() throws Exception
	{
		Class.forName("org.sqlite.JDBC");
		con = DriverManager.getConnection(url, user, password);
		st = con.createStatement();
		
		//rs = st.executeQuery("SELECT VERSION()");
		/*if (rs.next()) {
			System.out.println(rs.getString(1));
		}*/
	}

	@Override
	public void createCollection(String collectionName) throws Exception
	{
		String type = "bytea";
		String post = ""; // ENGINE=InnoDB;
		st.execute("CREATE TABLE "+collectionName+" (ID	INTEGER, a "+type+", b "+type+", c "+type+")" + post);
	}

	@Override
	public void createDocument(String collectionName, int key, byte[] data0, byte[] data1, byte[] data2) throws Exception
	{
		String sql = "INSERT INTO " + collectionName + " VALUES (?,?,?,?)";
		PreparedStatement statement = con.prepareStatement(sql);
		statement.setInt(1, key);
		statement.setBytes(2, data0);
		statement.setBytes(3, data1);
		statement.setBytes(4, data2);
		statement.execute();
		statement.close();
	}

	@Override
	public void readDocument(String collectionName, int key) throws Exception
	{
		rs = st.executeQuery("SELECT * FROM " + collectionName + " WHERE id=" + key);
		rs.getBytes(1);
		rs.getBytes(2);
		rs.getBytes(3);
	}

	@Override
	public void readValue(String collectionName, int key, int value) throws Exception
	{
		rs = st.executeQuery("SELECT a FROM " + collectionName + " WHERE id=" + key);
		rs.getBytes(1);
	}

	@Override
	public void updateDocument(String collectionName, int key, byte[] data0, byte[] data1, byte[] data2) throws Exception
	{
		String sql = "UPDATE " + collectionName + " SET a=?,b=?,c=? WHERE id=?";
		PreparedStatement statement = con.prepareStatement(sql);

		statement.setBytes(1, data0);
		statement.setBytes(2, data1);
		statement.setBytes(3, data2);
		statement.setInt(4, key);
		statement.execute();
		statement.close();
	}

	@Override
	public void updateValue(String collectionName, int key, byte[] data) throws Exception
	{
		String sql = "UPDATE " + collectionName + " SET a=? WHERE id=?";
		PreparedStatement statement = con.prepareStatement(sql);

		statement.setBytes(1, data);
		statement.setInt(2, key);
		statement.execute();
		statement.close();
	}

	@Override
	public void deleteDocument(String collectionName, int key) throws Exception
	{
		st.execute("DELETE FROM " + collectionName + " WHERE id=" + key);
	}

	@Override
	public void deleteCollection(String collectionName) throws Exception
	{
		st.execute("DROP TABLE " + collectionName);
	}

	@Override
	public void cleanup() throws Exception
	{
		con.close();
	}

}
