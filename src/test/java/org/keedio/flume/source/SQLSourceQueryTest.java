package org.keedio.flume.source;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class SQLSourceQueryTest {

  @Test
  public void chekGetAllRowsWithNullParam() {
    SQLSourceStatus sqlSourceStatus = new SQLSourceStatus("Source Name");
    SQLSourceQuery sqlSourceQuery = new SQLSourceQuery(sqlSourceStatus);
    assertEquals(new ArrayList<String>(), sqlSourceQuery.getAllRows(null));
  }

  @Test
  public void chekGetAllRowsWithEmptyParam() {

    SQLSourceStatus sqlSourceStatus = new SQLSourceStatus("Source Name");
    SQLSourceQuery sqlSourceQuery = new SQLSourceQuery(sqlSourceStatus);
    assertEquals(new ArrayList<String>(), sqlSourceQuery.getAllRows(new ArrayList<List<Object>>()));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void chekGetAllRows() {

    SQLSourceStatus sqlSourceStatus = new SQLSourceStatus("Source Name");
    SQLSourceQuery sqlSourceQuery = new SQLSourceQuery(sqlSourceStatus);
    List<List<Object>> queryResult = new ArrayList<List<Object>>(2);
    List<String[]> expectedResult = new ArrayList<String[]>(2);
    String string1 = "string1";
    String string2 = "string2";
    int int1 = 1;
    int int2 = 2;
    Date date1 = new Date(115, 0, 1);
    Date date2 = new Date(115, 1, 2);

    List<Object> row1 = new ArrayList<Object>(3);
    String[] expectedRow1 = new String[3];
    row1.add(string1);
    expectedRow1[0] = string1;
    row1.add(int1);
    expectedRow1[1] = Integer.toString(int1);
    row1.add(date1);
    expectedRow1[2] = date1.toString();
    queryResult.add(row1);
    expectedResult.add(expectedRow1);

    List<Object> row2 = new ArrayList<Object>(3);
    String[] expectedRow2 = new String[3];
    row2.add(string2);
    expectedRow2[0] = string2;
    row2.add(int2);
    expectedRow2[1] = Integer.toString(int2);
    row2.add(date2);
    expectedRow2[2] = date2.toString();
    queryResult.add(row2);
    expectedResult.add(expectedRow2);

    assertArrayEquals(expectedResult.get(0), sqlSourceQuery.getAllRows(queryResult).get(0));
    assertArrayEquals(expectedResult.get(1), sqlSourceQuery.getAllRows(queryResult).get(1));
  }
}
