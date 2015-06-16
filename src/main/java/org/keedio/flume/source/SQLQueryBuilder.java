package org.keedio.flume.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLQueryBuilder {
	
	private static final Logger log = LoggerFactory.getLogger(SQLQueryBuilder.class);
	
	public static String buildQuery(SQLSourceUtils sqlUtils){
		
    	String query;
    	String incrColumName= sqlUtils.getIncrementalColumnName();
    	Long incrValue = sqlUtils.getIncrementalValue();
    	String table = sqlUtils.getTable();
    	String columns = sqlUtils.getColumnsToSelect();
    	JdbcDriver driver = JdbcDriver.valueOf(sqlUtils.getDriverName().toUpperCase());
    	int maxRows = sqlUtils.getMaxRows();
		
		String where = incrColumName + ">"	+ incrValue;
		
		
		if (sqlUtils.getCustomQuery() == null){
			
			query = null;
			
			switch(driver){
				case MYSQL:
					query = "SELECT " + columns + " FROM " + table + " WHERE " 
							+ where + " LIMIT " + maxRows + ";";
					break;
				case DB2:
					break;
				case ORACLE:
					break;
				case POSTGRESQL:
					break;
				case SQLITE:
					break;
				case SQLSERVER:
					query = "SELECT TOP " + maxRows + " " + columns + " FROM " 
							+ table + " WHERE "	+ where + ";";
					break;
				default:
					break;
					
			};
		}
		else{
			query = buildCustomQuery(sqlUtils, where);
		}
		return query;
	}
	
	public static String buildCustomQuery(SQLSourceUtils sqlUtils ,String where){
		String customQuery = sqlUtils.getCustomQuery();
		if (!customQuery.contains("@")){
			log.error("Check custom query, it must contents @ where incremental value should be appears");
			return null;
		}
		else{
			customQuery = customQuery.replace("@", where);
			return customQuery;
		}
	}
}
