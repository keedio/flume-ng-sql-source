/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  
 * http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *******************************************************************************/
package org.apache.flume.source;


import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.apache.flume.metrics.SqlSourceCounter;



/**
 * A Source to read data from a SQL database. This source ask for new data in a table each configured time.
 * An incremental column is required to use this source (e.g timestamp), every time some data is imported
 * the source counter is increased to start importing new rows<p>
 * <tt>type: </tt> org.apache.flume.source.SQLSource <p>
 * <tt>connection.url: </tt> database connnection URL <p>
 * <tt>user: </tt> user to connect to database <p>
 * <tt>password: </tt> user password <p>
 * <tt>table: </tt> table to read from <p>
 * <tt>columns.to.select </tt> columns to select for import data (* will import all) <p>
 * <tt>incremental.column.name </tt> column name for incremental import <p>
 * <tt>incremental.value </tt> TODO: Change this to read from file the value <p>
 * <tt>run.query.delay </tt> delay time to do each query to database <p>
 * 
 * @author Marcelo Valle https://github.com/mvalleavila
 */
public class SQLSource extends AbstractSource implements Configurable, PollableSource {
    
    private static final Logger log = LoggerFactory.getLogger(SQLSource.class);
    private SqlDBEngine mDBEngine;
    private SQLSourceUtils sqlSourceUtils;
    private boolean isConnected;
    private JdbcDriver driver;
    private Statement statement;
    private SqlSourceCounter sqlSourceCounter;
       
    @Override
    public void configure(Context context) {
              
        log.info("Reading and processing configuration values for source " + getName());
        sqlSourceUtils = new SQLSourceUtils(context);
        sqlSourceCounter = new SqlSourceCounter("SOURCESQL." + this.getName());
        sqlSourceCounter.start();
        mDBEngine = new SqlDBEngine(sqlSourceUtils.getConnectionURL(),
                                    sqlSourceUtils.getUserDataBase(),
                                    sqlSourceUtils.getPasswordDatabase());
        
        log.info("Establishing connection to database " + sqlSourceUtils.getDataBase() 
        		+ " for source  " + getName());
       
        if (loadDriver(sqlSourceUtils.getDriverName())) {
            log.info("Source " + getName() + " Connected to " + sqlSourceUtils.getDataBase());
        } else {
            log.error("Error loading driver " + getSqlSourceUtils().getDriverName());
        }
        
    } //end configure
    
    
    public Status process() throws EventDeliveryException {
    	byte[] message;
    	Event event;
    	Map<String, String> headers;

    	try
    	{
    		String where = " WHERE " + sqlSourceUtils.getIncrementalColumnName() + ">" 
    				+ sqlSourceUtils.getCurrentIncrementalValue();
    		String query = "SELECT " + sqlSourceUtils.getColumnsToSelect() + " FROM " 
    				+ sqlSourceUtils.getTable() + where + " ORDER BY "
    				+ sqlSourceUtils.getIncrementalColumnName() + ";";

    		log.info("Query: " + query);
    		ResultSet queryResult = mDBEngine.runQuery(query,this.statement);
    		String queryResultRow;
    		ResultSetMetaData mMetaData = queryResult.getMetaData();
    		int mNumColumns = mMetaData.getColumnCount();
                    
    		//retrieve each row from resultset
    		while(queryResult.next()){
    			sqlSourceCounter.incrementRowsCount();
    			queryResultRow ="";

    			for(int i = 1; i <= mNumColumns-1; i++){
    				queryResultRow = queryResultRow + queryResult.getString(i) +",";
    			}

    			queryResultRow = queryResultRow + queryResult.getString(mNumColumns);
    			message = queryResultRow.getBytes();
    			event = new SimpleEvent();
    			headers = new HashMap<String, String>();
    			headers.put("timestamp", String.valueOf(System.currentTimeMillis()));
    			event.setBody(message);
    			event.setHeaders(headers);
    			getChannelProcessor().processEvent(event);
    			sqlSourceCounter.incrementEventCount();
    			sqlSourceCounter.incrementRowsProc();
    		}
    		//A TYPE_FORWARD_ONLY ResultSet doesnot support method last() , among others.
    		if (!(this.sqlSourceUtils.getDriverName().equals("sqlite"))){
    			if ( queryResult.last())
    			{
    				sqlSourceUtils.setCurrentIncrementalValue(Long.parseLong(
    						queryResult.getString(sqlSourceUtils.getIncrementalColumnName()),10));
    				
    				log.info("Last row increment value readed: " + sqlSourceUtils.getIncrementalValue() 
    						+ ", updating status file...");
    				sqlSourceUtils.updateStatusFile(sqlSourceUtils.getIncrementalValue());
    			} 
    		} else {
    			ResultSet r = statement.executeQuery("SELECT COUNT(*) AS rowcount FROM " 
    		+ this.sqlSourceUtils.getTable() + ";");
    			r.next();
    			int count = r.getInt("rowcount");
    			if (count > this.sqlSourceUtils.getStatusFileIncrement()){
    				sqlSourceUtils.setCurrentIncrementalValue(Long.parseLong(queryResult.getString(1),10));
    				
    				log.info("Last row increment value readed: " + sqlSourceUtils.getIncrementalValue() 
    						+ ", updating status file...");
    				sqlSourceUtils.updateStatusFile(sqlSourceUtils.getIncrementalValue());
    			}
    			r.close();
    		}

    		Thread.sleep(sqlSourceUtils.getRunQueryDelay());				
    		return Status.READY;
    	}
    	
    	catch(SQLException e)
    	{
    		log.error("SQL exception, check if query for source " + getName() + " is correctly constructed");
                    
    		return Status.BACKOFF;
            }
            catch(InterruptedException e)
            {
                    log.error("Interruptedexception", e);
                    return Status.BACKOFF;			
            }
    }

 
    public void start(Context context) {
            log.info("Starting sql source {} ...", getName());
        super.start();
    }

    @Override
    public void stop() {
        
            log.info("Stopping sql source {} ...", getName());
            try {
                    if (isConnected) {
                        log.info("Closing connection to database " + sqlSourceUtils.getConnectionURL());
                        mDBEngine.CloseConnection();
                    } else {
                        log.info("Nothing to close for " + sqlSourceUtils.getConnectionURL());
                    }
                    
            } catch (SQLException e) {
                    log.error("Error closing database connection " + sqlSourceUtils.getConnectionURL());
                    super.stop();
                    
            }
            this.sqlSourceCounter.stop();
            super.stop();
    }

    /*
    @return SQLSourceUtils
    */
    public SQLSourceUtils getSqlSourceUtils(){
        return sqlSourceUtils;
    }
    
    /*
    @return boolean driver register itself in DriverManager
        default option will be used by jdbc drivers > 4.0
    */
    
    private boolean loadDriver(String driverName)  {
        try {
        	driver = JdbcDriver.valueOf(driverName.toUpperCase());
            switch(driver){
                case POSTGRESQL:
                    Class.forName("org.postgresql.Driver");
                    mDBEngine.EstablishConnection();
                    statement = mDBEngine.getConnection().createStatement(
                    		ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);                    
                    isConnected = true;
                    break;
                    
                case MYSQL:
                    Class.forName("com.mysql.jdbc.Driver");
                    mDBEngine.EstablishConnection();
                    statement = mDBEngine.getConnection().createStatement();                    
                    isConnected = true;
                    break;
                
                case SQLITE:
                    Class.forName("org.sqlite.JDBC"); 
                    //sqlite does not support user neither pass
                    mDBEngine = new SqlDBEngine(sqlSourceUtils.getConnectionURL()); 
                    mDBEngine.EstablishConnection();
                    statement = mDBEngine.getConnection().createStatement(
                    		ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                    isConnected = true;
                    break;
                    
                case SQLSERVER:
                    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");                    
                    mDBEngine.EstablishConnection();
                    statement = mDBEngine.getConnection().createStatement(
                    		ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                    isConnected = true;
                    break;
                
                case ORACLE:
                    Class.forName("oracle.jdbc.OracleDriver");
                    mDBEngine.EstablishConnection();
                    statement = mDBEngine.getConnection().createStatement();                    
                    isConnected = true;
                    break;
                    
                case DERBY:
                    Class.forName("com.ibm.db2.jcc.DB2Driver");
                    mDBEngine.EstablishConnection();
                    statement = mDBEngine.getConnection().createStatement();                    
                    isConnected = true;
                    break;
                    
                default:                   
                     mDBEngine.EstablishConnection();  
                     statement = mDBEngine.getConnection().createStatement();
                     isConnected = true;
                    break;
            }
            
        } catch (SQLException e) {
                log.error("Error establishing connection to database for driver " 
                		+ getSqlSourceUtils().getDriverName());
                isConnected = false;
                
        } catch (ClassNotFoundException e){
                log.error("Error resgistering dinamic load driver " + getSqlSourceUtils().getDriverName());
                isConnected = false;
                
        }
        return isConnected;
    }
}
