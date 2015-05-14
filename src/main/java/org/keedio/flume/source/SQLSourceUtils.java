package org.keedio.flume.source;


import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;





/**
 *  * @author Marcelo Valle https://github.com/mvalleavila
 *  * @modified Luis Lazaro
 */
public class SQLSourceUtils {
	private static final Logger log = LoggerFactory.getLogger(SQLSourceUtils.class);
	private String statusFilePath, statusFileName, connectionURL, table,
                       incrementalColumnName,columnsToSelect,user,password,driverName,
                       customQuery;
	private int runQueryDelay,batchSize,maxRows;
	private long incrementalValue;
	private File file,directory;
	private static final String DEFAULT_STATUS_DIRECTORY = "/var/lib/flume";
	
	public SQLSourceUtils(Context context) throws ConfigurationException {
		statusFilePath = context.getString("status.file.path", DEFAULT_STATUS_DIRECTORY);
		statusFileName = context.getString("status.file.name");
		connectionURL = context.getString("connection.url");
		table = context.getString("table");
		incrementalColumnName = context.getString("incremental.column.name");
		columnsToSelect = context.getString("columns.to.select","*");
		runQueryDelay = context.getInteger("run.query.delay",10000);
		user = context.getString("user");
		password = context.getString("password");
		directory = new File(getStatusFilePath());
		customQuery = context.getString("custom.query");
		batchSize = context.getInteger("batch.size",100);
		maxRows = context.getInteger("max.rows",10000);
		
		setDriverNameFromURL();
		checkMandatoryProperties();
                
		if (!(isStatusDirectoryCreated())) {
			createDirectory();
		}
		file = new File(getStatusFilePath()+"/"+getStatusFileName());
		
		incrementalValue = getStatusFileIncrement(context.getLong("incremental.value",0L));
	}
	
	public void updateStatusFile(ResultSet queryResult) throws NumberFormatException, SQLException{
		
		log.info("Updating status file");
		
		if (queryResult.isAfterLast())
			queryResult.last();
		
		setCurrentIncrementalValue(Long.parseLong(queryResult.getString(getIncrementalColumnName()),10));
		
		log.info("Last row increment value readed: " + getIncrementalValue() 
				+ ", updating status file...");
		
		writeStatusFile(getIncrementalValue());		
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
        
	
	private long getStatusFileIncrement(long configuredStartValue){
		
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
						statusInfo[2].equals(incrementalColumnName)){
					reader.close();
					log.info(statusFilePath + "/" + statusFileName + " correctly formed");				
					return Long.parseLong(statusInfo[3],10);
				}
				else{
					log.warn(statusFilePath + "/" + statusFileName + " corrupt!!! Deleting it.");
					reader.close();
					deleteStatusFile();
					return configuredStartValue;
				}
			}catch (NumberFormatException | IOException e){
				log.error("Corrupt increment value in file!!! Deleting it.");
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
	
	private void writeStatusFile(long incrementalValue){
		
		/* Status file creation or update */
		try{
			Writer writer = new FileWriter(file,false);
			writer.write(connectionURL+" ");
			writer.write(table+" ");
			writer.write(incrementalColumnName+" ");
			writer.write(Long.toString(incrementalValue)+" \n");
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
		if (getIncrementalColumnName() == null){
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
	@void set statusFilePath
	 */
	private void setStatusFilePath(String newStatusFilePath){
		statusFilePath  = newStatusFilePath;
	}
        
	/*
	@return String statusFileName
	 */
	private String getStatusFileName(){
		return statusFileName;
	}
        
	/*
	@void set statusFileName
	 */
	private void setStatusFileName(String newStatusFileName){
		statusFileName = newStatusFileName;
	}
       
	/*
	@return String connectionURL
	 */
	public String getConnectionURL(){            
		return connectionURL;
	}
        
	/*
	 * @void set connectionURL
	 */
	private void setConnectionURL(String newConnectionURL) {
		connectionURL = newConnectionURL;
	}

	/*
	 * @return String table
	 */
	public String getTable() {
		return table;
	}

	/*
	 * @void set table
	 */
	private void setTable(String newTable) {
		table = newTable;
	}

	/*
	 * @return String incrementalColumnName
	 */
	public String getIncrementalColumnName() {
		return incrementalColumnName;
	}

	/*
	 * @void set incrementalColumnName
	 */
	private void setIncrementalColumnName(String newIncrementalColumnName) {
		incrementalColumnName = newIncrementalColumnName;
	}

	/*
	 * @return File directory
	 */
	public File getDirectory() {
		return directory;
	}

	/*
	 * @void set directory
	 */
	private void setDirectory(File newDirectory) {
		directory = newDirectory;
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
	public String getColumnsToSelect() {
		return columnsToSelect;
	}

	/*
	 * @return the custom query defined in properties file
	 */
	public String getCustomQuery() {
		return customQuery;
	}

	/*
	 * @void set columns to select from data base
	 */
	public void setColumnsToSelect(String newColumns) {
		columnsToSelect = newColumns;
	}

	/*
	 * @return long incremental value as parameter from this
	 */
	public long getIncrementalValue() {
		return incrementalValue;
	}

	/*
	 * @void set incrementValue
	 */
	public void setCurrentIncrementalValue(long newValue) {
		incrementalValue = newValue;
	}

	/*
	 * @return String user for database
	 */
	public String getUserDataBase() {
		return user;
	}

	/*
	 * @return String passwor for user
	 */
	public String getPasswordDatabase() {
		return password;
	}

	/*
	 * @return int delay in ms
	 */
	public int getRunQueryDelay() {
		return runQueryDelay;
	}

	/*
	 * return String driver name jdbc
	 */
	public String getDriverName() {
		return driverName;
	}
	
	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	public int getMaxRows() {
		return maxRows;
	}

	public void setMaxRows(int maxRows) {
		this.maxRows = maxRows;
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
	
}
