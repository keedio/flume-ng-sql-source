package org.keedio.flume.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLQueryBuilder {
	
	private static final Logger log = LoggerFactory.getLogger(SQLQueryBuilder.class);
	
	public static String buildQuery(SQLSourceHelper sqlUtils){
		
    	String table = sqlUtils.getTable();
    	String columns = sqlUtils.getColumnsToSelect();
		
    	return "SELECT " + columns + " FROM " + table;
		
	}
	
	public static String buildCustomQuery(SQLSourceHelper sqlUtils ,String where){
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
