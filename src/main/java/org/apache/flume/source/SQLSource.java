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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
	
	private static MySqlDBEngine mDBEngine;
	private static MySqlDao mDAO;
	
	private String table, columnsToSelect,incrementalColumnName;
	private int runQueryDelay;
	private long incrementalValue;
	private SQLSourceUtils sqlSourceUtils;
	
	public Status process() throws EventDeliveryException {
		List<Event> eventList = new ArrayList<Event>();
		byte[] message;
		Event event;
		Map<String, String> headers;
		
		try
		{
			String where = " WHERE " + incrementalColumnName + ">" + incrementalValue;
			String query = "SELECT " + columnsToSelect + " FROM " + table + where + " ORDER BY "+ incrementalColumnName + ";";
			
			log.debug("Query: " + query);
			Vector<Vector<String>> queryResult = mDAO.runQuery(query);
			Vector<String> columns = mDAO.getColumns();
			
			boolean columnPosFind;
			String queryResultRow;

			columnPosFind = false;
			int incrementalColumnPosition=0;
			do
			{
				if (columns.get(incrementalColumnPosition).equals(incrementalColumnName))
					columnPosFind=true;
				else
					incrementalColumnPosition++;
			}while(!columnPosFind);
				

			if (!queryResult.isEmpty())
			{
				incrementalValue = Long.parseLong(queryResult.lastElement().get(incrementalColumnPosition),10);
				log.info("New values append to database, query result: \n" + queryResult.toString());
					
				for (int i=0;i<queryResult.size();i++)
				{
					queryResultRow = queryResult.get(i).toString();
					queryResultRow = queryResultRow.substring(1, queryResultRow.length()-1);
					message = queryResultRow.getBytes();
	                event = new SimpleEvent();
	                headers = new HashMap<String, String>();
	                headers.put("timestamp", String.valueOf(System.currentTimeMillis()));
	                log.debug("Message: {}", new String(message));
	                event.setBody(message);
	                event.setHeaders(headers);
	                eventList.add(event);
				}
				getChannelProcessor().processEventBatch(eventList);
				log.info("Last row increment value readed: " + incrementalValue + ", updating status file...");
				sqlSourceUtils.updateStatusFile(incrementalValue);
			}
			Thread.sleep(runQueryDelay);				
            return Status.READY;
		}
		catch(SQLException e)
		{
			log.error("SQL exception, check if query is correctly constructed");
			e.printStackTrace();
			return Status.BACKOFF;
		}
		catch(InterruptedException e)
		{
			e.printStackTrace();
			return Status.BACKOFF;			
		}
	}


	public void start(Context context) {
		log.info("Starting sql source {} ...", this);
	    super.start();	    
	}

	@Override
	public synchronized void stop() {
		log.info("Stopping sql source {} ...", this);
		try {
			log.info("Closing database connection");
			mDBEngine.CloseConnection();
		} catch (SQLException e) {
			log.error("Error closing database connection");
			super.stop();
			e.printStackTrace();
		}
		super.stop();
	}

	@Override
	public void configure(Context context) {
		
	    String connectionURL, user, password;
		
	    log.info("Reading and processing configuration values");
	    
		connectionURL = context.getString("connection.url");
		user = context.getString("user");
		password = context.getString("password");
		table = context.getString("table");
		columnsToSelect = context.getString("columns.to.select", "*");
		incrementalColumnName = context.getString("incremental.column.name");
		
		sqlSourceUtils = new SQLSourceUtils(context);		
		
		/* Get incremental value form file is exist, if not the starting value to select table
		   will be readed from configuration file */
		
		incrementalValue = sqlSourceUtils.getCurrentIncrementalValue();
		
		runQueryDelay = context.getInteger("run.query.delay");		
		
		mDBEngine = new MySqlDBEngine(connectionURL, user, password);
		log.info("Establishing connection to database");
		try {
			mDBEngine.EstablishConnection();
			mDAO= new MySqlDao(mDBEngine.getConnection());
		} catch (SQLException e) {
			log.error("Error establishing connection to database");
			e.printStackTrace();
		}
	}
}
