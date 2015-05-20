package org.keedio.flume.source;


import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.Context;
import org.hibernate.cfg.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *  * @author Marcelo Valle https://github.com/mvalleavila
 *  * @modified Luis Lazaro
 */

public class SQLSourceHelper {
	private static final Logger log = LoggerFactory.getLogger(SQLSourceHelper.class);
	private String statusFilePath, statusFileName, connectionURL, table,
                       indexColumnName,columnsToSelect,user,password,driverName,
                       customQuery;
	private int runQueryDelay,batchSize,maxRows;
	private int currentIndex;
	private File file,directory;
	private Configuration config;
	private String query;
	
	private static final String DEFAULT_STATUS_DIRECTORY = "/var/lib/flume";
	private static final int DEFAULT_QUERY_DELAY = 10000;
	private static final int DEFAULT_BATCH_SIZE = 100;
	private static final int DEFAULT_MAX_ROWS = 10000;
	private static final int DEFAULT_INCREMENTAL_VALUE = 0;
	
	

	
	public SQLSourceHelper(Context context) throws ConfigurationException {
		
		statusFilePath = context.getString("status.file.path", DEFAULT_STATUS_DIRECTORY);
		statusFileName = context.getString("status.file.name");
		connectionURL = context.getString("connection.url");
		table = context.getString("table");
		indexColumnName = context.getString("incremental.column.name");
		columnsToSelect = context.getString("columns.to.select","*");
		runQueryDelay = context.getInteger("run.query.delay",DEFAULT_QUERY_DELAY);
		user = context.getString("user");
		password = context.getString("password");
		directory = new File(getStatusFilePath());
		customQuery = context.getString("custom.query");
		batchSize = context.getInteger("batch.size",DEFAULT_BATCH_SIZE);
		maxRows = context.getInteger("max.rows",DEFAULT_MAX_ROWS);
		
		checkMandatoryProperties();
		setDriverNameFromURL();
                
		if (!(isStatusDirectoryCreated())) {
			createDirectory();
		}
		file = new File(getStatusFilePath() + "/" + getStatusFileName());
		
		currentIndex = getStatusFileIndex(context.getInteger("incremental.value",DEFAULT_INCREMENTAL_VALUE));
		
		query = SQLQueryBuilder.buildQuery(this);
	}

	
	public void updateStatusFile(){
		writeStatusFile();		
	}
	
	private boolean isStatusFileCreated(){
		
		return file.exists() && !file.isDirectory() ? true: false;
	}
        
	/*
	@return boolean abstract File as directory if exists
	 */
	private boolean isStatusDirectoryCreated(){
		return directory.exists() && !directory.isFile() ? true: false;
	}
        
	
	private int getStatusFileIndex(int configuredStartValue){
		
		if (!isStatusFileCreated()){
			log.info("Status file not created, using start value from config file");
			return configuredStartValue;
		}
		else{
			try {
				FileReader reader = new FileReader(file);
				char[] chars = new char[(int) file.length()];
				reader.read(chars);
				String[] statusInfo = new String(chars).split(" ");
				if (statusInfo[0].equals(connectionURL) && statusInfo[1].equals(table) &&
						statusInfo[2].equals(indexColumnName)){
					reader.close();
					log.info(statusFilePath + "/" + statusFileName + " correctly formed");				
					return Integer.parseInt(statusInfo[3]);
				}
				else{
					log.warn(statusFilePath + "/" + statusFileName + " corrupt!!! Deleting it.");
					reader.close();
					deleteStatusFile();
					return configuredStartValue;
				}
			}catch (NumberFormatException | IOException e){
				log.error("Corrupt index value in file!!! Deleting it.");
				deleteStatusFile();
				return configuredStartValue;
			}
		}
	}
	
	private void deleteStatusFile(){
		if (file.delete()){
			log.info("Deleted status file: {}",file.getAbsolutePath());
		}else{
			log.warn("Error deleting file: {}",file.getAbsolutePath());
		}
			
	}
	
	private void writeStatusFile(){
		
		/* Status file creation or update */
		try{
			Writer writer = new FileWriter(file,false);
			writer.write(connectionURL+" ");
			writer.write(table+" ");
			writer.write(indexColumnName+" ");
			writer.write(Integer.toString(getCurrentIndex())+" \n");
			writer.close();
		}catch (IOException e) {
			log.error("Error writing incremental value to status file!!!");
			
		}
	}
	
	private void checkMandatoryProperties() throws ConfigurationException {
		
		if (getStatusFileName() == null){
			throw new ConfigurationException("status.file.name property not set");
		}
		if (getConnectionURL() == null){
			throw new ConfigurationException("connection.url property not set");
		}
		if (getTable() == null && getCustomQuery() == null){
			throw new ConfigurationException("property table not set");
		}
		if (getIndexColumnName() == null){
			throw new ConfigurationException("incremental.column.name  property not set");
		}
		if (getPasswordDatabase() == null){
			throw new ConfigurationException("password property not set");
		}
		if (getUserDataBase() == null){
			throw new ConfigurationException("user property not set");
		}
	}
        
	/*
	@return String statusFilePath
	 */
	private String getStatusFilePath(){
		return statusFilePath;
	}
        
	/*
	@return String statusFileName
	 */
	private String getStatusFileName(){
		return statusFileName;
	}
       
	/*
	@return String connectionURL
	 */
	String getConnectionURL(){            
		return connectionURL;
	}

	/*
	 * @return String table
	 */
	String getTable() {
		return table;
	}

	/*
	 * @return String incrementalColumnName
	 */
	String getIndexColumnName() {
		return indexColumnName;
	}

	/*
	 * @return File directory
	 */
	private File getDirectory() {
		return directory;
	}

	/*
	 * @return boolean pathname into directory
	 */
	private boolean createDirectory() {
		return getDirectory().mkdir();
	}

	/*
	 * @return String columns to select from table data base
	 */
	String getColumnsToSelect() {
		return columnsToSelect;
	}

	/*
	 * @return the custom query defined in properties file
	 */
	String getCustomQuery() {
		return customQuery;
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
	String getUserDataBase() {
		return user;
	}

	/*
	 * @return String password for user
	 */
	String getPasswordDatabase() {
		return password;
	}

	/*
	 * @return int delay in ms
	 */
	int getRunQueryDelay() {
		return runQueryDelay;
	}

	/*
	 * return String driver name jdbc
	 */
	String getDriverName() {
		return driverName;
	}
	
	int getBatchSize() {
		return batchSize;
	}

	int getMaxRows() {
		return maxRows;
	}
	
	private void setDriverNameFromURL() {
		String[] stringsURL = connectionURL.split(":");
		if (stringsURL[0].equals("jdbc")) {
			driverName = stringsURL[1];
			log.info("SQL Driver name: "+driverName);
		} else {
			log.warn("Error: impossible to get driver name from  "
					+ getConnectionURL());
			driverName = "unknown";
		}
	}
	
	Configuration getHibernateConfig(){
		return config;
	}
	
	String getQuery(){
		return query;
	}
	
}
