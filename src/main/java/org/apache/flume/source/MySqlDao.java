package org.apache.flume.source;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;

import com.mysql.jdbc.MySQLConnection;

/**
 * Is used as the database access object to return a result set of a given query.
 * @author david
 *
 */
public class MySqlDao {

	private MySQLConnection mConnection;
	private ResultSetMetaData mMetaData;
	private Vector<String> mColumns;
	
	/**
	 * Constructor for the DAO get a valid connection.
	 * @param Connection
	 */
	public MySqlDao(MySQLConnection Connection){
		this.mConnection = Connection;
	}
	
	/**
	 * Runs the query and returns a Vector of Vectors as a result
	 * @param mQuery
	 * @return Vector<Vector<String>>
	 * @throws SQLException
	 */
	public Vector<Vector<String>> runQuery(String mQuery) throws SQLException{
		Vector<Vector<String>> mResults = new Vector<Vector<String>>();
		Statement mStatement = (Statement) this.mConnection.createStatement();
		ResultSet mResultSet = mStatement.executeQuery(mQuery);
		mMetaData = mResultSet.getMetaData();
		
		int mNumColumns = mMetaData.getColumnCount();
		setColumns(mNumColumns,mMetaData);
		
		while(mResultSet.next()){
			Vector<String> mRow = new Vector<String>();
			for(int i = 1; i <= mNumColumns; i++){
				mRow.add(mResultSet.getString(i));
			}
			mResults.add(mRow);
		}
		return mResults;
	}
	
	/**
	 * Retrieves the columns the were returned in the previous query.
	 * @return Vector<String>
	 * @throws SQLException
	 */
	public Vector<String> getColumns() throws SQLException{
		return this.mColumns;
	}
	
	/**
	 * Runs an update query e.g.(insert/delete)
	 * @param mQuery
	 * @return row count
	 * @throws SQLException
	 */
	public int runUpdate(String mQuery) throws SQLException{
		Statement mStatement = this.mConnection.createStatement();
		return mStatement.executeUpdate(mQuery);
	}
	
	/**
	 * Sets the columns variable.
	 * @param mNumColumns
	 * @param mMetaData
	 * @throws SQLException
	 */
	public void setColumns(int mNumColumns, ResultSetMetaData mMetaData) throws SQLException{
		mColumns = new Vector<String>();
		for(int i = 1; i <= mNumColumns; i++){
			mColumns.add(mMetaData.getColumnName(i));
		}
	}
}
