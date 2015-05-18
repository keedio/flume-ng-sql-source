package org.keedio.flume.source;

import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Sets up connection to database.
 * @author Luis Lázaro
 *
 */
public class SqlDBEngine  {
	private String mURL;
	private String mUser;
	private String mPassword;
	private Connection mConnection;
	
	/**
	 * Constructor to set the URL, Username, and Password for the DB.
	 * @param URL
	 * @param Username
	 * @param Password
	 */
	public SqlDBEngine(String URL, String Username, String Password){
		this.mURL = URL;
		this.mUser = Username;
		this.mPassword = Password;
	}
        
    public SqlDBEngine(String URL){
            this.mURL= URL;
    }
	
	/**
	 * Establishes the database connection.
	 * @throws SQLException
	 */
	public void EstablishConnection() throws SQLException{
		mConnection =  DriverManager.getConnection(this.mURL, this.mUser, this.mPassword);
	}
	
	/**
	 * Closes the database connection.
	 * @throws SQLException
	 */
	public void CloseConnection() throws SQLException{
		mConnection.close();
	}
	
	/**
	 * Retrieves the current connection.
	 * @return
	 */
	public Connection getConnection(){
		return this.mConnection;
	}
       
	
	/*
	@return Resultset
	*/
	public ResultSet runQuery(String mQuery, Statement statement) throws SQLException{
		return statement.executeQuery(mQuery);
	}
}
