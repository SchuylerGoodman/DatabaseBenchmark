package dbtesting;

import java.sql.*;

/**
 *
 * @author thomas
 */
abstract class SQLTest extends Test {

	Connection con = null;
	Statement st = null;
	ResultSet rs = null;

	String url;
	String postCreate = "";

	String user = "admin";
	String password = "";

    protected String type = "bytea";

	SQLTest(String url) {
		this.url = url;
	}

	@Override
	public void init() throws Exception
	{
		con = DriverManager.getConnection(url, user, password);
		st = con.createStatement();
		
		//rs = st.executeQuery("SELECT VERSION()");
		/*if (rs.next()) {
			System.out.println(rs.getString(1));
		}*/
	}

	@Override
	public long createCollection(String collectionName) throws Exception
	{
        tick();
		st.execute("CREATE TABLE "+collectionName+" (ID	INTEGER, a "+type+", b "+type+", c "+type+")" + postCreate);
        return tock();
	}

	@Override
	public long createDocument(String collectionName, int key, byte[] data0, byte[] data1, byte[] data2) throws Exception
	{
		String sql = "INSERT INTO " + collectionName + " VALUES (?,?,?,?)";
		PreparedStatement statement = con.prepareStatement(sql);
		statement.setInt(1, key);
		statement.setBytes(2, data0);
		statement.setBytes(3, data1);
		statement.setBytes(4, data2);
        tick();
		statement.execute();
		statement.close();
        return tock();
	}

	@Override
	public long readDocument(String collectionName, int key) throws Exception
	{
        tick();
		rs = st.executeQuery("SELECT * FROM " + collectionName + " WHERE id=" + key);
        return tock();
	}

	@Override
	public long readValue(String collectionName, int key, int value) throws Exception
	{
        tick();
		rs = st.executeQuery("SELECT a FROM " + collectionName + " WHERE id=" + key);
        return tock();
	}

	@Override
	public long updateDocument(String collectionName, int key, byte[] data0, byte[] data1, byte[] data2) throws Exception
	{
		String sql = "UPDATE " + collectionName + " SET a=?,b=?,c=? WHERE id=?";
		PreparedStatement statement = con.prepareStatement(sql);

		statement.setBytes(1, data0);
		statement.setBytes(2, data1);
		statement.setBytes(3, data2);
		statement.setInt(4, key);
        tick();
		statement.execute();
		statement.close();
        return tock();
	}

	@Override
	public long updateValue(String collectionName, int key, byte[] data) throws Exception
	{
		String sql = "UPDATE " + collectionName + " SET a=? WHERE id=?";
		PreparedStatement statement = con.prepareStatement(sql);

		statement.setBytes(1, data);
		statement.setInt(2, key);
        tick();
		statement.execute();
		statement.close();
        return tock();
	}

	@Override
	public long deleteDocument(String collectionName, int key) throws Exception
	{
        tick();
		st.execute("DELETE FROM " + collectionName + " WHERE id=" + key);
        return tock();
	}

	@Override
	public long deleteCollection(String collectionName) throws Exception
	{
        tick();
		st.execute("DROP TABLE " + collectionName);
        return tock();
	}

	@Override
	public void cleanup() throws Exception
	{
		con.close();
	}

}
