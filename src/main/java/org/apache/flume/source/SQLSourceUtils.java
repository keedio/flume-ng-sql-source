package org.apache.flume.source;


import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

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
                       database;
        private int runQueryDelay;
	private long incrementalValue;
	private File file,directory;
	private FileWriter writer;
        private final String defaultDirectory = "/var/lib/flume";
	
	public SQLSourceUtils(Context context) {
		statusFilePath = context.getString("status.file.path", defaultDirectory);
		statusFileName = context.getString("status.file.name");
		connectionURL = context.getString("connection.url");
		table = context.getString("table");
                database = context.getString("database");
		incrementalColumnName = context.getString("incremental.column.name");
                columnsToSelect = context.getString("columns.to.select");
                runQueryDelay = context.getInteger("run.query.delay");
		incrementalValue = context.getLong("incremental.value");
                user = context.getString("user");
		password = context.getString("password");
                directory = new File(getStatusFilePath());
                
                if (!(isStatusDirectoryCreated())) {
                    createDirectory();
                }
		file = new File(getStatusFilePath()+"/"+getStatusFileName());
	}
	
	public long getCurrentIncrementalValue(){

            if (!isStatusFileCreated()){
                    log.info(statusFilePath + "/" + statusFileName + " not exists, creating it "
                            + "and using configured incremental value: {}",incrementalValue);
                    writeStatusFile(incrementalValue);
                    return incrementalValue;
            }
            else{
                    log.info(statusFilePath + "/" + statusFileName + " currently exists, checking it");
                    long incValueInFile = getStatusFileIncrement(); 
                    if (incValueInFile < 0){
                            log.error("There was an error getting value from file. Creating "
                                    + "new status file and using configured incremental value: {}",incrementalValue);
                            writeStatusFile(incrementalValue);
                            return incrementalValue;
                    }
                    else{
                            log.info("Incremental value readed from file: {}",incValueInFile);
                            return incValueInFile;
                    }
            }
	}
	
	public void updateStatusFile(long lastIncrementalvalue){
		
			log.info("Updating status file");
			writeStatusFile(lastIncrementalvalue);		
	}
	
	public boolean isStatusFileCreated(){
		
		return file.exists() && !file.isDirectory() ? true: false;
	}
        
        /*
        @return boolean abstract File as directory if exists
        */
        private boolean isStatusDirectoryCreated(){
            return directory.exists() && !directory.isFile() ? true: false;
        }
        
	
	public long getStatusFileIncrement(){
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
				log.warn(statusFilePath + "/" + statusFileName + " corrupt!!!");
				reader.close();
				return -1;
			}
		}catch (NumberFormatException e){
			log.error("Corrupt increment value in file!!!");
			return -1;
		}catch (IOException e){
			log.error("Error reading incremental value from status file!!!");
			
			return -1;
		}
	}
	
	public void writeStatusFile(long incrementalValue){
		
		/* Status file creation or update */
		try{
			writer = new FileWriter(file,false);
			writer.write(connectionURL+" ");
			writer.write(table+" ");
			writer.write(incrementalColumnName+" ");
			writer.write(Long.toString(incrementalValue)+" \n");
			writer.close();
		}catch (IOException e) {
			log.error("Error writing incremental value to status file!!!");
			
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
        @void set connectionURL
        */
        private void setConnectionURL(String newConnectionURL){
            connectionURL = newConnectionURL;
        }
                
        /*
        @return String table
        */
        public String getTable(){
            return table;
        }
        
        /*
        @void set table
        */
        private void setTable(String newTable){
            table = newTable;
        }
        
        /*
        @return String incrementalColumnName
        */
        public String getIncrementalColumnName(){
            return incrementalColumnName;
        }
        
        /*
        @void set incrementalColumnName
        */
        private void setIncrementalColumnName(String newIncrementalColumnName){
            incrementalColumnName = newIncrementalColumnName;
        }
        
        /*
        @return File file
        */
        public File getFile(){
            return file;
        }
        
        /*
        @void set file
        */
        private void setFile(File newFile){
            file = newFile;
        }
        
        /*
        @return File directory
        */
        public File getDirectory(){
            return directory;
        }
        
        /* 
        @void set directory
        */
        private void setDirectory(File newDirectory){
            directory = newDirectory;
        }
        
        /*
        @return boolean pathname into directory
        */
        private boolean createDirectory(){
            return getDirectory().mkdir();
        }
        
        /*
        @return String columns to select from table data base
        */
        public String getColumnsToSelect(){
            return columnsToSelect;
        }
        
        /*
        @void set columns to select from data base
        */
        public void setColumnsToSelect(String newColumns)
        {
            columnsToSelect = newColumns;
        }
        
        /*
        @return long incremental value as parameter from this
        */
        public long getIncrementalValue(){
            return incrementalValue;
        }
        /*
        @void set incrementValue
        */
        public void setCurrentIncrementalValue(long newValue){
            incrementalValue = newValue;
        }
        
        /*
        @return String user for database
        */
        public String getUserDataBase(){
            return user;
        }
        
        /*
        @return String passwor for user
        */
        public String getPasswordDatabase(){
            return password;
        }
        
        /*
        @return int delay in ms
        */
        public int getRunQueryDelay(){
            return runQueryDelay;
        }
        
        /*
        return String driver name jdbc
        */
        public String getDriverName(){
            String[] stringsURL = connectionURL.split(":");
            if (stringsURL[0].equals("jdbc")){
                driverName = stringsURL[1];
            } else {
                log.info("Error: impossible to get driver name from  " + getConnectionURL() );
                driverName = "unknown";
            }
            return driverName;
        }
        
        /*
        @return String data base name
        */
        public String getDataBase(){
            return database;
        }
}
