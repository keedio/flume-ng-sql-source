package org.keedio.flume.source;

import static org.json.simple.parser.ParseException.ERROR_UNEXPECTED_EXCEPTION;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper to manage configuration parameters and utility methods
 *
 * <p>Configuration parameters readed from flume configuration file: <tt>type: </tt>
 * org.keedio.flume.source.SQLSource
 *
 * <p><tt>table: </tt> table to read from
 *
 * <p><tt>columns.to.select: </tt> columns to select for import data (* will import all)
 *
 * <p><tt>run.query.delay: </tt> delay time to execute each query to database
 *
 * <p><tt>status.file.path: </tt> Directory to save status file
 *
 * <p><tt>status.file.name: </tt> Name for status file (saves last row index processed)
 *
 * <p><tt>batch.size: </tt> Batch size to send events from flume source to flume channel
 *
 * <p><tt>max.rows: </tt> Max rows to import from DB in one query
 *
 * <p><tt>custom.query: </tt> Custom query to execute to database (be careful)
 *
 * <p>
 *
 * @author <a href="mailto:mvalle@keedio.com">Marcelo Valle</a>
 * @author <a href="mailto:lalazaro@keedio.com">Luis Lazaro</a>
 */
public class SQLSourceStatus {

  private static final Logger LOG = LoggerFactory.getLogger(SQLSourceStatus.class);

  private static final String SOURCE_NAME_STATUS_FILE = "SourceName";
  private static final String URL_STATUS_FILE = "URL";
  private static final String COLUMNS_TO_SELECT_STATUS_FILE = "ColumnsToSelect";
  private static final String TABLE_STATUS_FILE = "Table";
  private static final String LAST_INDEX_STATUS_FILE = "LastIndex";
  private static final String QUERY_STATUS_FILE = "Query";

  private final String statusFilePath;
  private final String statusFileName;
  private final String connectionURL;
  private final String table;
  private final String columnsToSelect;
  private final String customQuery;
  private final String sourceName;
  private final String startFrom;

  private File file;

  private String currentIndex;
  private String query;

  private Map<String, String> statusFileJsonMap = new LinkedHashMap<>();

  /**
   * Builds an SQLSourceStatus containing the configuration parameters and usefull utils for SQL
   * Source
   *
   * @param sourceName source file name for store status
   */
  public SQLSourceStatus(String sourceName) {
    this.sourceName = sourceName;

    statusFilePath = FlumeContext.getInstance().getStatusFilePath();
    statusFileName = FlumeContext.getInstance().getStatusFileName();
    table = FlumeContext.getInstance().getTable();
    columnsToSelect = FlumeContext.getInstance().getColumnsToSelect();
    customQuery = FlumeContext.getInstance().getCustomQuery();
    connectionURL = FlumeContext.getInstance().getConnectionURL();

    startFrom = FlumeContext.getInstance().getStartFrom();

    applyConfig();

    query = buildQuery();
  }

  private void applyConfig() {

    try {
      FileUtils.forceMkdir(new File(statusFilePath));
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
    }
    file = new File(statusFilePath + "/" + statusFileName);

    if (!isStatusFileCreated()) {
      currentIndex = startFrom;
      createStatusFile();
    } else {
      currentIndex = getStatusFileIndex(startFrom);
    }
  }

  public String buildQuery() {

    if (customQuery == null) {
      return "SELECT " + columnsToSelect + " FROM " + table;
    } else {
      if (customQuery.contains("$@$")) {
        return customQuery.replace("$@$", currentIndex);
      } else {
        return customQuery;
      }
    }
  }

  private boolean isStatusFileCreated() {
    return file.exists() && !file.isDirectory() ? true : false;
  }

  /** Create status file */
  public void createStatusFile() {

    statusFileJsonMap.put(SOURCE_NAME_STATUS_FILE, sourceName);
    statusFileJsonMap.put(URL_STATUS_FILE, connectionURL);
    statusFileJsonMap.put(LAST_INDEX_STATUS_FILE, currentIndex);

    if (isCustomQuerySet()) {
      statusFileJsonMap.put(QUERY_STATUS_FILE, customQuery);
    } else {
      statusFileJsonMap.put(COLUMNS_TO_SELECT_STATUS_FILE, columnsToSelect);
      statusFileJsonMap.put(TABLE_STATUS_FILE, table);
    }

    try {
      Writer fileWriter = new FileWriter(file, false);
      JSONValue.writeJSONString(statusFileJsonMap, fileWriter);
      fileWriter.close();
    } catch (IOException e) {
      LOG.error("Error creating value to status file!!!", e);
    }
  }

  /** Update status file with last read row index */
  public void updateStatusFile() {

    statusFileJsonMap.put(LAST_INDEX_STATUS_FILE, currentIndex);

    try {
      Writer fileWriter = new FileWriter(file, false);
      JSONValue.writeJSONString(statusFileJsonMap, fileWriter);
      fileWriter.close();
    } catch (IOException e) {
      LOG.error("Error writing incremental value to status file!!!", e);
    }
  }

  private String getStatusFileIndex(String configuredStartValue) {

    if (!isStatusFileCreated()) {
      LOG.info("Status file not created, using start value from config file and creating file");
      return configuredStartValue;
    } else {
      try {
        FileReader fileReader = new FileReader(file);

        JSONParser jsonParser = new JSONParser();
        statusFileJsonMap = (Map) jsonParser.parse(fileReader);
        checkJsonValues();
        return statusFileJsonMap.get(LAST_INDEX_STATUS_FILE);

      } catch (Exception e) {
        LOG.error("Exception reading status file, doing back up and creating new status file", e);
        backupStatusFile();
        return configuredStartValue;
      }
    }
  }

  private void checkJsonValues() throws ParseException {

    // Check commons values to default and custom query
    if (!statusFileJsonMap.containsKey(SOURCE_NAME_STATUS_FILE)
        || !statusFileJsonMap.containsKey(URL_STATUS_FILE)
        || !statusFileJsonMap.containsKey(LAST_INDEX_STATUS_FILE)) {
      LOG.error("Status file doesn't contains all required values");
      throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
    }
    if (!statusFileJsonMap.get(URL_STATUS_FILE).equals(connectionURL)) {
      LOG.error("Connection url in status file doesn't match with configured in properties file");
      throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
    } else if (!statusFileJsonMap.get(SOURCE_NAME_STATUS_FILE).equals(sourceName)) {
      LOG.error("Source name in status file doesn't match with configured in properties file");
      throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
    }

    // Check default query values
    if (customQuery == null) {
      if (!statusFileJsonMap.containsKey(COLUMNS_TO_SELECT_STATUS_FILE)
          || !statusFileJsonMap.containsKey(TABLE_STATUS_FILE)) {
        LOG.error("Expected ColumsToSelect and Table fields in status file");
        throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
      }
      if (!statusFileJsonMap.get(COLUMNS_TO_SELECT_STATUS_FILE).equals(columnsToSelect)) {
        LOG.error(
            "ColumsToSelect value in status file doesn't match with configured in properties file");
        throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
      }
      if (!statusFileJsonMap.get(TABLE_STATUS_FILE).equals(table)) {
        LOG.error("Table value in status file doesn't match with configured in properties file");
        throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
      }
      return;
    }

    // Check custom query values
    if (customQuery != null) {
      if (!statusFileJsonMap.containsKey(QUERY_STATUS_FILE)) {
        LOG.error("Expected Query field in status file");
        throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
      }
      if (!statusFileJsonMap.get(QUERY_STATUS_FILE).equals(customQuery)) {
        LOG.error("Query value in status file doesn't match with configured in properties file");
        throw new ParseException(ERROR_UNEXPECTED_EXCEPTION);
      }
      return;
    }
  }

  private void backupStatusFile() {
    file.renameTo(
        new File(statusFilePath + "/" + statusFileName + ".bak." + System.currentTimeMillis()));
  }

  /*
   * @return long incremental value as parameter from this
   */
  String getCurrentIndex() {
    return currentIndex;
  }

  /*
   * @void set incrementValue
   */
  void setCurrentIndex(String newValue) {
    currentIndex = newValue;
  }

  String getQuery() {
    return query;
  }

  boolean isCustomQuerySet() {
    return (customQuery != null);
  }
}
