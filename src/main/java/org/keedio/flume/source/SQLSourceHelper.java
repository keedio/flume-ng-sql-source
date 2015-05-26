package org.keedio.flume.source;


import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *  Helper to manage configuration parameters and utility methods
 * 
 *  @author <a href="mailto:mvalle@keedio.com">Marcelo Valle</a>
 *  @modified Luis Lazaro
 */

public class SQLSourceHelper {
	
	private static final Logger LOG = LoggerFactory.getLogger(SQLSourceHelper.class);
	
	private File file,directory;
	private int runQueryDelay, batchSize, maxRows, currentIndex;
	private String statusFilePath, statusFileName, connectionURL, table,
    columnsToSelect, user, password, customQuery, query;
	
	private static final String DEFAULT_STATUS_DIRECTORY = "/var/lib/flume";
	private static final int DEFAULT_QUERY_DELAY = 10000;
	private static final int DEFAULT_BATCH_SIZE = 100;
	private static final int DEFAULT_MAX_ROWS = 10000;
	private static final int DEFAULT_INCREMENTAL_VALUE = 0;

	/**
	 * Builds an SQLSourceHelper containing the configuration parameters and
	 * usefull utils for SQL Source
	 * @param context Flume source context, contains the properties from configuration file
	 */
	public SQLSourceHelper(Context context){
		
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
		
		checkMandatoryProperties();
                
		if (!(isStatusDirectoryCreated())) {
			createDirectory();
		}
		
		file = new File(statusFilePath + "/" + statusFileName);
		
		currentIndex = getStatusFileIndex(context.getInteger("incremental.value",DEFAULT_INCREMENTAL_VALUE));
		
		query = buildQuery();
	}
	
	
	private String buildQuery() {
		
		if (customQuery == null)
	    	return "SELECT " + columnsToSelect + " FROM " + table;
		else
			return customQuery;
	}

	
	private boolean isStatusFileCreated(){
		
		return file.exists() && !file.isDirectory() ? true: false;
	}
	
	private boolean isStatusDirectoryCreated() {
		return directory.exists() && !directory.isFile() ? true: false;
	}
	
	/**
	 * Converter from List<List<Object>> query result to List<String[]>
	 * for csvWriter
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
				row[j] = rawRow.get(j).toString();
			}
			allRows.add(row);
		}
		
		return allRows;
	}
	
        
	public void updateStatusFile() {
		/* Status file creation or update */
		try {
			Writer writer = new FileWriter(file,false);
			writer.write(connectionURL+" ");
			writer.write(table+" ");
			writer.write(Integer.toString(currentIndex)+" \n");
			writer.close();
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
				FileReader reader = new FileReader(file);
				char[] chars = new char[(int) file.length()];
				reader.read(chars);
				String[] statusInfo = new String(chars).split(" ");
				if (statusInfo[0].equals(connectionURL) && statusInfo[1].equals(table)) {
					reader.close();
					LOG.info(statusFilePath + "/" + statusFileName + " correctly formed");				
					return Integer.parseInt(statusInfo[2]);
				}
				else{
					LOG.warn(statusFilePath + "/" + statusFileName + " corrupt!!! Deleting it.");
					reader.close();
					deleteStatusFile();
					return configuredStartValue;
				}
			} catch (NumberFormatException | IOException e) {
				LOG.error("Corrupt index value in file!!! Deleting it.", e);
				deleteStatusFile();
				return configuredStartValue;
			}
		}
	}
	
	private void deleteStatusFile() {
		if (file.delete()){
			LOG.info("Deleted status file: {}",file.getAbsolutePath());
		}else{
			LOG.warn("Error deleting file: {}",file.getAbsolutePath());
		}
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
	
}
