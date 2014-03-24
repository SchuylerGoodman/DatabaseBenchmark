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
	String url = "jdbc:sqlite://localhost/test";
	String user = "admin";
	String password = "";

	@Override
	public void init() throws Exception
	{
		con = DriverManager.getConnection(url, user, password);
		st = con.createStatement();
		rs = st.executeQuery("SELECT VERSION()");

		if (rs.next()) {
			System.out.println(rs.getString(1));
		}
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
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public void readValue(String collectionName, int key, int value) throws Exception
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

}
