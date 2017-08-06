package org.keedio.flume.source;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.flume.Context;
import org.junit.Before;
import org.junit.Test;

public class FlumeContextTest {
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
  }

  @Test(expected = IllegalArgumentException.class)
  public void checkStatusFileNameNotSet() {
    when(context.getString("status.file.name")).thenReturn(null);
    FlumeContext.getInstance().init(context);
  }

  @Test(expected = IllegalArgumentException.class)
  public void connectionURLNotSet() {
    when(context.getString("hibernate.connection.url")).thenReturn(null);
    FlumeContext.getInstance().init(context);
  }

  @Test(expected = IllegalArgumentException.class)
  public void tableNotSet() {
    when(context.getString("table")).thenReturn(null);
    FlumeContext.getInstance().init(context);
  }
}
