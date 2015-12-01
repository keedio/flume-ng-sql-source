package org.keedio.flume.source;


import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import static org.json.simple.parser.ParseException.*;

import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *  Helper to manage configuration parameters and utility methods <p>
 * 
 * Configuration parameters readed from flume configuration file:
 * <tt>type: </tt> org.keedio.flume.source.SQLSource <p>
 * <tt>connection.url: </tt> database connection URL <p>
 * <tt>user: </tt> user to connect to database <p>
 * <tt>password: </tt> user password <p>
 * <tt>table: </tt> table to read from <p>
 * <tt>columns.to.select: </tt> columns to select for import data (* will import all) <p>
 * <tt>incremental.value: </tt> Start value to import data <p>
 * <tt>run.query.delay: </tt> delay time to execute each query to database <p>
 * <tt>status.file.path: </tt> Directory to save status file <p>
 * <tt>status.file.name: </tt> Name for status file (saves last row index processed) <p>
 * <tt>batch.size: </tt> Batch size to send events from flume source to flume channel <p>
 * <tt>max.rows: </tt> Max rows to import from DB in one query <p>
 * <tt>custom.query: </tt> Custom query to execute to database (be careful) <p>
 * 
 *  @author <a href="mailto:mvalle@keedio.com">Marcelo Valle</a>
 *  @author <a href="mailto:lalazaro@keedio.com">Luis Lazaro</a>
 */

public class SQLSourceHelper {
	
	private static final Logger LOG = LoggerFactory.getLogger(SQLSourceHelper.class);
	
	private File file,directory;
	private int runQueryDelay, batchSize, maxRows, currentIndex;
	private String statusFilePath, statusFileName, connectionURL, table,
    columnsToSelect, user, password, customQuery, query, hibernateDialect,
    hibernateDriver, agentName, sourceName, statusColumn, startFrom;

	private boolean hibernateAutocommit;
	
	private static final String DEFAULT_STATUS_DIRECTORY = "/var/lib/flume";
	private static final int DEFAULT_QUERY_DELAY = 10000;
	private static final int DEFAULT_BATCH_SIZE = 100;
	private static final int DEFAULT_MAX_ROWS = 10000;
	private static final int DEFAULT_INCREMENTAL_VALUE = 0;
	
	private static final String SOURCE_NAME_STATUS_FILE = "SourceName";
	private static final String URL_STATUS_FILE = "URL";
	private static final String COLUMNS_TO_SELECT_STATUS_FILE = "ColumnsToSelect";
	private static final String TABLE_STATUS_FILE = "Table";
	private static final String LAST_INDEX_STATUS_FILE = "LastIndex";
	private static final String STATUS_COLUMN_NAME_STATUS_FILE = "StatusColumn";
	private static final String QUERY_STATUS_FILE = "Query";

	/**
	 * Builds an SQLSourceHelper containing the configuration parameters and
	 * usefull utils for SQL Source
	 * @param context Flume source context, contains the properties from configuration file
	 */
	public SQLSourceHelper(Context context, String sourceName){
		
		statusFilePath = context.getString("status.file.path", DEFAULT_STATUS_DIRECTORY);
		statusFileName = context.getString("status.file.name");
		connectionURL = context.getString("connection.url");
		table = context.getString("table");
		columnsToSelect = context.getString("columns.to.select","*");
		runQueryDelay = context.getInteger("run.query.delay",DEFAULT_QUERY_DELAY);
		user = context.getString("user");
		password = context.getString("password");
		directory = new File(statusFilePath);
		customQuery = context.getString("custom.query");
		batchSize = context.getInteger("batch.size",DEFAULT_BATCH_SIZE);
		maxRows = context.getInteger("max.rows",DEFAULT_MAX_ROWS);
		hibernateDialect = context.getString("hibernate.dialect");
		hibernateDriver = context.getString("hibernate.connection.driver_class");
		hibernateAutocommit = Boolean.valueOf(context.getString("hibernate.connection.autocommit"));
		this.sourceName = sourceName;
		statusColumn = context.getString("status.column");
		startFrom = context.getString("start.from");
		
		checkMandatoryProperties();
                
		if (!(isStatusDirectoryCreated())) {
			createDirectory();
		}
		
		file = new File(statusFilePath + "/" + statusFileName);
		
		currentIndex = getStatusFileIndex(context.getInteger(startFrom, DEFAULT_INCREMENTAL_VALUE));
		
		query = buildQuery();
	}
	
	
	private String buildQuery() {
		
		if (customQuery == null){
			return "SELECT " + columnsToSelect + " FROM " + table;
		}
		else {
			if (customQuery.contains("$@$")){
				return customQuery.replace("$@$", Integer.toString(currentIndex));
			}
			else{
				return customQuery;
			}
		}
	}

	
	private boolean isStatusFileCreated(){
		return file.exists() && !file.isDirectory() ? true: false;
	}
	
	private boolean isStatusDirectoryCreated() {
		return directory.exists() && !directory.isFile() ? true: false;
	}
	
	/**
	 * Converter from a List of Object List to a List of String arrays <p>
	 * Useful for csvWriter
	 * @param queryResult Query Result from hibernate executeQuery method
	 * @return A list of String arrays, ready for csvWriter.writeall method
	 */
	public List<String[]> getAllRows(List<List<Object>> queryResult){
		
		List<String[]> allRows = new ArrayList<String[]>();
		
		if (queryResult == null || queryResult.isEmpty())
			return allRows;
		
		String[] row=null;
		
		for (int i=0; i<queryResult.size();i++)
		{	
			List<Object> rawRow = queryResult.get(i);
			row = new String[rawRow.size()];
			for (int j=0; j< rawRow.size(); j++){
				if (rawRow.get(j) != null)
					row[j] = rawRow.get(j).toString();
				else
					row[j] = "";
			}
			allRows.add(row);
		}
		
		return allRows;
	}
	
	/**
	 * Create status file
	 */
	public void createStatusFile(){
		Map<String,String> jsonMap = new LinkedHashMap<String,String>();
		
		if (customQuery == null){
			jsonMap.put(SOURCE_NAME_STATUS_FILE, agentName);
			jsonMap.put(URL_STATUS_FILE, connectionURL);
			jsonMap.put(COLUMNS_TO_SELECT_STATUS_FILE, columnsToSelect);
			jsonMap.put(TABLE_STATUS_FILE, table);
			jsonMap.put(LAST_INDEX_STATUS_FILE, Integer.toString(currentIndex));
		}
		else
		{
			jsonMap.put(SOURCE_NAME_STATUS_FILE, agentName);
			jsonMap.put(URL_STATUS_FILE, connectionURL);
			jsonMap.put(QUERY_STATUS_FILE, customQuery);
			jsonMap.put(STATUS_COLUMN_NAME_STATUS_FILE, table);
			jsonMap.put(LAST_INDEX_STATUS_FILE, Integer.toString(currentIndex));
		}
		try {
			Writer fileWriter = new FileWriter(file,false);
			JSONValue.writeJSONString(jsonMap, fileWriter);
			fileWriter.close();
		} catch (IOException e) {
			LOG.error("Error creating value to status file!!!",e);
		}
	}
	
    /**
     * Update status file with last read row index    
     */
	public void updateStatusFile() {
		
		Map<String,String> jsonMap = new LinkedHashMap<String,String>();
		
		if (customQuery == null){
			jsonMap.put(LAST_INDEX_STATUS_FILE, Integer.toString(currentIndex));
		}
		try {
			Writer fileWriter = new FileWriter(file,false);
			JSONValue.writeJSONString(jsonMap, fileWriter);
			fileWriter.close();
		} catch (IOException e) {
			LOG.error("Error writing incremental value to status file!!!",e);
		}	
	}
	
	private int getStatusFileIndex(int configuredStartValue) {
		
		if (!isStatusFileCreated()) {
			LOG.info("Status file not created, using start value from config file");
			return configuredStartValue;
		}
		else{
			try {
				FileReader fileReader = new FileReader(file);
				
				JSONParser jsonParser = new JSONParser();
				Map<String,String> jsonMap = (Map)jsonParser.parse(fileReader);
				checkJsonValues(jsonMap);
				return Integer.parseInt(jsonMap.get(LAST_INDEX_STATUS_FILE));
				
			} catch (Exception e) {
				LOG.error("Exception reading status file, doing back up and creating new status file", e);
				backupStatusFile();
				return configuredStartValue;
			}
		}
	}
	
	private void checkJsonValues(Map<String,String> jsonMap) throws ParseException {
		
		// Check commons values to default and custom query
		if (!jsonMap.containsKey(SOURCE_NAME_STATUS_FILE) || !jsonMap.containsKey(URL_STATUS_FILE) ||
			!jsonMap.containsKey(LAST_INDEX_STATUS_FILE)) {
			LOG.error("Status file doesn't contains all required values");	
			throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
		}
		if (jsonMap.get(URL_STATUS_FILE) != connectionURL){
			LOG.error("Connection url in status file doesn't match with configured in properties file");
			throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
		}else if (jsonMap.get(SOURCE_NAME_STATUS_FILE) != sourceName){
			LOG.error("Source name in status file doesn't match with configured in properties file");
			throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
		}
		else if (jsonMap.get(LAST_INDEX_STATUS_FILE) != sourceName){
			LOG.error("Source name in status file doesn't match with configured in properties file");
			throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
		}
		
		// Check default query values
		if (customQuery == null)
		{
			if (!jsonMap.containsKey(COLUMNS_TO_SELECT_STATUS_FILE) || !jsonMap.containsKey(TABLE_STATUS_FILE)){
				LOG.error("Expected ColumsToSelect and Table fields in status file");	
				throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
			}
			if (jsonMap.get(COLUMNS_TO_SELECT_STATUS_FILE) != columnsToSelect){
				LOG.error("ColumsToSelect value in status file doesn't match with configured in properties file");
				throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
			}
			if (jsonMap.get(TABLE_STATUS_FILE) != table){
				LOG.error("Table value in status file doesn't match with configured in properties file");
				throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
			}
			return;
		}
		
		// Check custom query values
		if (customQuery != null){
			if (!jsonMap.containsKey(QUERY_STATUS_FILE) || !jsonMap.containsKey(STATUS_COLUMN_NAME_STATUS_FILE)){
				LOG.error("Expected Query and StatusColumn fields in status file");	
				throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
			}
			if (jsonMap.get(QUERY_STATUS_FILE) != query){
				LOG.error("Query value in status file doesn't match with configured in properties file");
				throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
			}
			if (jsonMap.get(STATUS_COLUMN_NAME_STATUS_FILE) != statusColumn){
				LOG.error("StatuColumn value in status file doesn't match with configured in properties file");
				throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
			}
			return;
		}
	}

	private void backupStatusFile() {
		file.renameTo(new File(statusFilePath + "/" + statusFileName + ".bak." + System.currentTimeMillis()));		
	}
	
	private void checkMandatoryProperties() {
		
		if (statusFileName == null){
			throw new ConfigurationException("status.file.name property not set");
		}
		if (connectionURL == null){
			throw new ConfigurationException("connection.url property not set");
		}
		if (table == null && customQuery == null){
			throw new ConfigurationException("property table not set");
		}
		if (customQuery != null && statusColumn ==null){
			throw new ConfigurationException("status.column.name property not set");
		}
		if (password == null){
			throw new ConfigurationException("password property not set");
		}
		if (user == null){
			throw new ConfigurationException("user property not set");
		}
	}

	/*
	 * @return String connectionURL
	 */
	String getConnectionURL() {            
		return connectionURL;
	}

	/*
	 * @return boolean pathname into directory
	 */
	private boolean createDirectory() {
		return directory.mkdir();
	}

	/*
	 * @return long incremental value as parameter from this
	 */
	int getCurrentIndex() {
		return currentIndex;
	}

	/*
	 * @void set incrementValue
	 */
	void setCurrentIndex(int newValue) {
		currentIndex = newValue;
	}

	/*
	 * @return String user for database
	 */
	String getUser() {
		return user;
	}

	/*
	 * @return String password for user
	 */
	String getPassword() {
		return password;
	}

	/*
	 * @return int delay in ms
	 */
	int getRunQueryDelay() {
		return runQueryDelay;
	}
	
	int getBatchSize() {
		return batchSize;
	}

	int getMaxRows() {
		return maxRows;
	}
	
	String getQuery() {
		return query;
	}


	String getHibernateDialect() {
		return hibernateDialect;
	}


	String getHibernateDriver() {
		return hibernateDriver;
	}
	
	boolean isCustomQuerySet() {
		return (customQuery != null);
	}
        
	boolean isHibernateAutocommit()	{
		return hibernateAutocommit;
	}
}
