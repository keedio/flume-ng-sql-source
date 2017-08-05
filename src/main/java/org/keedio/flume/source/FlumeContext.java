package org.keedio.flume.source;

import com.google.common.base.Preconditions;
import org.apache.flume.Context;
import org.apache.flume.source.PollableSourceConstants;

public class FlumeContext {

  private static final String DEFAULT_STATUS_DIRECTORY = "/var/lib/flume";
  private static final int DEFAULT_QUERY_DELAY = 10000;
  private static final int DEFAULT_BATCH_SIZE = 100;
  private static final int DEFAULT_MAX_ROWS = 10000;
  private static final String DEFAULT_INCREMENTAL_VALUE = "0";
  private static final String DEFAULT_DELIMITER_ENTRY = ",";
  private static final Boolean DEFAULT_ENCLOSE_BY_QUOTES = true;

  private static FlumeContext instance;

  private Context context;

  private FlumeContext() {}

  public static FlumeContext getInstance() {
    if (instance == null) {
      instance = new FlumeContext();
    }

    return instance;
  }

  public void init(Context cxt) {
    context = cxt;
    Preconditions.checkArgument(
        null != context.getString("status.file.name"), "need status file name");
    Preconditions.checkArgument(
        null != context.getString("table") || null != context.getString("custom.query"),
        "property table not set");
    Preconditions.checkArgument(
        null != context.getString("hibernate.connection.url")
            || null != context.getString("hibernate.hikari.dataSource.url"),
        "hibernate.connection.url property not set");
  }

  public Context getContext() {
    return context;
  }

  public Boolean getReadOnlySession() {
    return context.getBoolean("read.only", false);
  }

  public String getStatusFilePath() {
    return context.getString("status.file.path", DEFAULT_STATUS_DIRECTORY);
  }

  public String getStatusFileName() {
    return context.getString("status.file.name");
  }

  public String getTable() {
    return context.getString("table");
  }

  public String getColumnsToSelect() {
    return context.getString("columns.to.select", "*");
  }

  public Integer getRunQueryDelay() {
    return context.getInteger("run.query.delay", DEFAULT_QUERY_DELAY);
  }

  public String getCustomQuery() {
    return context.getString("custom.query");
  }

  public Integer getBatchSize() {
    return context.getInteger("batch.size", DEFAULT_BATCH_SIZE);
  }

  public Integer getMaxRows() {
    return context.getInteger("max.rows", DEFAULT_MAX_ROWS);
  }

  public String getStartFrom() {
    return context.getString("start.from", DEFAULT_INCREMENTAL_VALUE);
  }

  public String getDelimiterEntry() {
    return context.getString("delimiter.entry", DEFAULT_DELIMITER_ENTRY);
  }

  public Boolean getEncloseByQuotes() {
    return context.getBoolean("enclose.by.quotes", DEFAULT_ENCLOSE_BY_QUOTES);
  }

  public String getConnectionURL() {
    String url = context.getString("hibernate.connection.url");
    if (null == url) {
      url = context.getString("hibernate.hikari.dataSource.url");
    }
    return url;
  }

  public Long getBackoffSleepIncrement() {
    return context.getLong(
        PollableSourceConstants.BACKOFF_SLEEP_INCREMENT,
        PollableSourceConstants.DEFAULT_BACKOFF_SLEEP_INCREMENT);
  }

  public Long getMaxBackoffSleep() {
    return context.getLong(
        PollableSourceConstants.MAX_BACKOFF_SLEEP,
        PollableSourceConstants.DEFAULT_MAX_BACKOFF_SLEEP);
  }
}
