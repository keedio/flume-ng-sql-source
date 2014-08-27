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
 */
public class SQLSourceUtils {
	private static final Logger log = LoggerFactory.getLogger(SQLSourceUtils.class);

	private String statusFilePath, statusFileName, connectionURL, table, incrementalColumnName;
	private long incrementalValue;
	private File file;
	private FileWriter writer;
	
	public SQLSourceUtils(Context context) {
		statusFilePath = context.getString("status.file.path", "/var/lib/flume");
		statusFileName = context.getString("status.file.name");
		connectionURL = context.getString("connection.url");
		table = context.getString("table");
		incrementalColumnName = context.getString("incremental.column.name");
		incrementalValue = context.getLong("incremental.value");
		file = new File(statusFilePath+"/"+statusFileName);
	}
	
	public long getCurrentIncrementalValue(){
		
		if (!isStatusFileCreated()){
			log.info(statusFilePath + "/" + statusFileName + " not exists, creating it and using configured incremental value: {}",incrementalValue);
			writeStatusFile(incrementalValue);
			return incrementalValue;
		}
		else{
			log.info(statusFilePath + "/" + statusFileName + " currently exists, checking it");
			long incValueInFile = getStatusFileIncrement(); 
			if (incValueInFile < 0){
				log.error("There was an error getting value from file. Creating new status file and using configured incremental value: {}",incrementalValue);
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
	
	private boolean isStatusFileCreated(){
		
		return file.exists() && !file.isDirectory() ? true: false;
	}
	
	private long getStatusFileIncrement(){
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
			e.printStackTrace();
			return -1;
		}
	}
	
	private void writeStatusFile(long incrementalValue){
		
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
			e.printStackTrace();
		}
	}
	
}
