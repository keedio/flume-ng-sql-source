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
package org.keedio.flume.source;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.keedio.flume.metrics.SqlSourceCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVWriter;


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
    protected SQLSourceUtils sqlSourceUtils;
    private boolean isConnected;
    private JdbcDriver driver;
    private Statement statement;
    private SqlSourceCounter sqlSourceCounter;
    private CSVWriter csvWriter;
       
    @Override
    public void configure(Context context) throws ConfigurationException{
        	
    	log.info("Reading and processing configuration values for source " + getName());
		
    	sqlSourceUtils = new SQLSourceUtils(context);
        
		sqlSourceCounter = new SqlSourceCounter("SOURCESQL." + this.getName());
        sqlSourceCounter.start();
        
        mDBEngine = new SqlDBEngine(sqlSourceUtils.getConnectionURL(),
                                    sqlSourceUtils.getUserDataBase(),
                                    sqlSourceUtils.getPasswordDatabase());
        
        log.info("Establishing connection to " + sqlSourceUtils.getConnectionURL() 
        		+ " for source  " + getName());
       
        csvWriter = new CSVWriter(new ChannelWriter());
        
        if (loadDriver(sqlSourceUtils.getDriverName())) {
            log.info("Source " + getName() + " CONNECTED to database");
        } else {
        	throw new ConfigurationException("Error loading driver " + sqlSourceUtils.getDriverName());
        }
        
    }  
    
    public Status process() throws EventDeliveryException {

    	String query = SQLQueryBuilder.buildQuery(sqlSourceUtils);
    	log.debug("Running query: " + query);
    	
    	try
    	{
    		ResultSet queryResult = mDBEngine.runQuery(query,this.statement);
    		
    		/* Checking if queryResult is not empty */
    		if (queryResult.isBeforeFirst())
    		{
	    		try{	
	    			csvWriter.writeAll(queryResult, false);
	    		}
	    		
	    		/* If csvWriter throws an SQL exception a row is corrupted 
	    		 * - Reports the error
	    		 * - Flush previously processed rows
	    		 * - Update status file to avoid row repetitions */
	    		
	    		catch (SQLException e) {
	                log.error(e.getMessage(),e);
	                csvWriter.flush();
	                sqlSourceUtils.updateStatusFile(queryResult);
	        		return Status.BACKOFF;
	    		}
	    		
	    		csvWriter.flush();	
	    		sqlSourceUtils.updateStatusFile(queryResult);
    		}
    		else{
    			log.debug("Empty ResultSet after running query");
    		}
    		
    		/* If queryResult size is equals to maxRows, there are pending rows to be processed
    		 * So the system doesn't wait to execute the next query */
    		
    		if (queryResult.getRow() < sqlSourceUtils.getMaxRows()){
    			Thread.sleep(sqlSourceUtils.getRunQueryDelay());
    		}
    		
    		queryResult.close();

    		return Status.READY;
    	}
    	
    	catch(SQLException e)
    	{
    		log.error("SQL exception, check if query for source " + getName() + " is correctly constructed");
            log.error(e.getMessage(),e);
    		return Status.BACKOFF;
        }
        catch(InterruptedException e)
        {
            log.error("Interruptedexception", e);
            return Status.BACKOFF;			
        }
    	catch(IOException e)
        {
            log.error("Error procesing row", e);
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
        
        try 
        {
            if (isConnected) {
                log.info("Closing connection to database " + sqlSourceUtils.getConnectionURL());
                mDBEngine.CloseConnection();
            } else {
                log.info("Nothing to close for " + sqlSourceUtils.getConnectionURL());
            }
            csvWriter.close();
                
        } catch (SQLException e) {
            log.warn("Error closing database connection " + sqlSourceUtils.getConnectionURL());
            try {
				csvWriter.close();
			} catch (IOException e1) {
				log.warn("Error CSVWriter object ", e);
			}
        } catch (IOException e) {
        	log.warn("Error CSVWriter object ", e);
        } finally {
        	this.sqlSourceCounter.stop();
        	super.stop();
        }
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
                    
                case DB2:
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
                log.error("Error establishing connection to database for driver {}: {}", 
                		sqlSourceUtils.getDriverName(), e.getMessage());
                isConnected = false;
                
        } catch (ClassNotFoundException e){
                log.error("Error resgistering dinamic load driver {}: {}",
                		sqlSourceUtils.getDriverName(), e.getMessage());
                isConnected = false;
                
        }
        return isConnected;
    }
    
    private class ChannelWriter extends Writer{
        private List<Event> events = new ArrayList<>();

        @Override
        public void write(char[] cbuf, int off, int len) throws IOException {
            Event event = new SimpleEvent();
            
            String s = new String(cbuf);
            event.setBody(s.substring(off, len-1).getBytes());
            
            Map<String, String> headers;
            headers = new HashMap<String, String>();
			headers.put("timestamp", String.valueOf(System.currentTimeMillis()));
			event.setHeaders(headers);
			
            events.add(event);
            
            if (events.size() >= sqlSourceUtils.getBatchSize())
            	flush();
        }

        @Override
        public void flush() throws IOException {
            getChannelProcessor().processEventBatch(events);
            events.clear();
        }

        @Override
        public void close() throws IOException {
            flush();
        }
    }
}
