package org.keedio.flume.source;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** @author Marcelo Valle https://github.com/mvalleavila */

//@RunWith(PowerMockRunner.class)
public class SQLSourceStatusTest {

  Context context = mock(Context.class);

  @Before
  public void setup() {

    when(context.getString("status.file.name")).thenReturn("statusFileName.txt");
    when(context.getString("hibernate.connection.url"))
        .thenReturn("jdbc:mysql://host:3306/database");
    when(context.getString("table")).thenReturn("table");
    when(context.getString("incremental.column.name")).thenReturn("incrementalColumName");
    when(context.getString("status.file.path", "/var/lib/flume")).thenReturn("/tmp/flume");
    when(context.getString("columns.to.select", "*")).thenReturn("*");
    when(context.getInteger("run.query.delay", 10000)).thenReturn(10000);
    when(context.getInteger("batch.size", 100)).thenReturn(100);
    when(context.getInteger("max.rows", 10000)).thenReturn(10000);
    when(context.getString("incremental.value", "0")).thenReturn("0");
    when(context.getString("start.from", "0")).thenReturn("0");
    FlumeContext.getInstance().init(context);
  }

  /*
  @Test
  public void checkNotCreatedDirectory() throws Exception {

  	SQLSourceStatus sqlSourceUtils = new SQLSourceStatus(context,"Source Name");
  	SQLSourceStatus sqlSourceUtilsSpy = PowerMockito.spy(sqlSourceUtils);

  	PowerMockito.verifyPrivate(sqlSourceUtilsSpy, Mockito.times(1)).invoke("createDirectory");
  }*/

  @Test
  public void getConnectionURL() {
    SQLSourceStatus sqlSourceStatus = new SQLSourceStatus("Source Name");
    assertEquals("jdbc:mysql://host:3306/database", FlumeContext.getInstance().getConnectionURL());
  }

  @Test
  public void getCurrentIndex() {
    SQLSourceStatus sqlSourceStatus = new SQLSourceStatus("Source Name");
    assertEquals("0", sqlSourceStatus.getCurrentIndex());
  }

  @Test
  public void setCurrentIndex() {
    SQLSourceStatus sqlSourceStatus = new SQLSourceStatus("Source Name");
    sqlSourceStatus.setCurrentIndex("10");
    assertEquals("10", sqlSourceStatus.getCurrentIndex());
  }

  @Test
  public void getRunQueryDelay() {
    SQLSourceStatus sqlSourceStatus = new SQLSourceStatus("Source Name");
    assertEquals(new Integer(10000), FlumeContext.getInstance().getRunQueryDelay());
  }

  @Test
  public void getBatchSize() {
    SQLSourceStatus sqlSourceStatus = new SQLSourceStatus("Source Name");
    assertEquals(new Integer(100), FlumeContext.getInstance().getBatchSize());
  }

  @Test
  public void getQuery() {
    SQLSourceStatus sqlSourceStatus = new SQLSourceStatus("Source Name");
    assertEquals("SELECT * FROM table", sqlSourceStatus.getQuery());
  }

  @Test
  public void getCustomQuery() {
    when(context.getString("custom.query")).thenReturn("SELECT column FROM table");
    when(context.getString("incremental.column")).thenReturn("incremental");
    SQLSourceStatus sqlSourceStatus = new SQLSourceStatus("Source Name");
    assertEquals("SELECT column FROM table", sqlSourceStatus.getQuery());
  }

  @SuppressWarnings("unused")
  @Test
  public void createDirectory() {

    SQLSourceStatus sqlSourceStatus = new SQLSourceStatus("Source Name");
    File file = new File("/tmp/flume");
    assertEquals(true, file.exists());
    assertEquals(true, file.isDirectory());
    if (file.exists()) {
      file.delete();
    }
  }

  @Test
  public void checkStatusFileCorrectlyCreated() {

    SQLSourceStatus sqlSourceStatus = new SQLSourceStatus("Source Name");
    //sqlSourceStatus.setCurrentIndex(10);

    sqlSourceStatus.updateStatusFile();

    File file = new File("/tmp/flume/statusFileName.txt");
    assertEquals(true, file.exists());
    if (file.exists()) {
      file.delete();
      file.getParentFile().delete();
    }
  }

  @Test
  public void checkStatusFileCorrectlyUpdated() throws Exception {

    //File file = File.createTempFile("statusFileName", ".txt");

    when(context.getString("status.file.path")).thenReturn("/var/lib/flume");
    when(context.getString("hibernate.connection.url"))
        .thenReturn("jdbc:mysql://host:3306/database");
    when(context.getString("table")).thenReturn("table");
    when(context.getString("status.file.name")).thenReturn("statusFileName");

    SQLSourceStatus sqlSourceStatus = new SQLSourceStatus("Source Name");
    sqlSourceStatus.createStatusFile();
    sqlSourceStatus.setCurrentIndex("10");

    sqlSourceStatus.updateStatusFile();

    SQLSourceStatus sqlSourceHelper2 = new SQLSourceStatus("Source Name");
    assertEquals("10", sqlSourceHelper2.getCurrentIndex());
  }

  @After
  public void deleteDirectory() {
    try {

      File file = new File("/tmp/flume");
      if (file.exists()) {
        FileUtils.deleteDirectory(file);
      }

    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
