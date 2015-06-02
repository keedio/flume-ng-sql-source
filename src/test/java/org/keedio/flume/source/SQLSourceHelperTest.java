package org.keedio.flume.source;

import java.io.File;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.conf.ConfigurationException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
* @author Marcelo Valle https://github.com/mvalleavila
*/

//@RunWith(PowerMockRunner.class)
public class SQLSourceHelperTest {

	Context context = mock(Context.class);

	@Before
	public void setup() {

		when(context.getString("status.file.name")).thenReturn("statusFileName.txt");
		when(context.getString("connection.url")).thenReturn("jdbc:mysql://host:3306/database");
		when(context.getString("table")).thenReturn("table");
		when(context.getString("incremental.column.name")).thenReturn("incrementalColumName");
		when(context.getString("user")).thenReturn("user");
		when(context.getString("password")).thenReturn("password");
		when(context.getString("status.file.path", "/var/lib/flume")).thenReturn("/tmp/flume");
		when(context.getString("columns.to.select", "*")).thenReturn("*");
		when(context.getInteger("run.query.delay", 10000)).thenReturn(10000);
		when(context.getInteger("batch.size", 100)).thenReturn(100);
		when(context.getInteger("max.rows", 10000)).thenReturn(10000);
		when(context.getLong("incremental.value", 0L)).thenReturn(0L);
	}

	@Test(expected = ConfigurationException.class)
	public void checkStatusFileNameNotSet() {
		when(context.getString("status.file.name")).thenReturn(null);
		new SQLSourceHelper(context);
	}

	@Test(expected = ConfigurationException.class)
	public void connectionURLNotSet() {
		when(context.getString("connection.url")).thenReturn(null);
		new SQLSourceHelper(context);
	}

	@Test(expected = ConfigurationException.class)
	public void tableNotSet() {
		when(context.getString("table")).thenReturn(null);
		new SQLSourceHelper(context);
	}

	@Test(expected = ConfigurationException.class)
	public void userNotSet() {
		when(context.getString("user")).thenReturn(null);
		new SQLSourceHelper(context);
	}

	@Test(expected = ConfigurationException.class)
	public void passwordNotSet() {
		when(context.getString("password")).thenReturn(null);
		new SQLSourceHelper(context);
	}

	/*
	@Test
	public void checkNotCreatedDirectory() throws Exception {

		SQLSourceHelper sqlSourceUtils = new SQLSourceHelper(context);
		SQLSourceHelper sqlSourceUtilsSpy = PowerMockito.spy(sqlSourceUtils);

		PowerMockito.verifyPrivate(sqlSourceUtilsSpy, Mockito.times(1)).invoke("createDirectory");
	}*/
	
	@Test
	public void chekGetAllRowsWithNullParam() {
		
		SQLSourceHelper sqlSourceHelper = new SQLSourceHelper(context);
		assertEquals(new ArrayList<String>(),sqlSourceHelper.getAllRows(null));
		
	}
	
	@Test
	public void chekGetAllRowsWithEmptyParam() {
		
		SQLSourceHelper sqlSourceHelper = new SQLSourceHelper(context);
		assertEquals(new ArrayList<String>(),sqlSourceHelper.getAllRows(new ArrayList<List<Object>>()));
	}
	
	@SuppressWarnings("deprecation")
	@Test
	public void chekGetAllRows() {

		SQLSourceHelper sqlSourceHelper = new SQLSourceHelper(context);
		List<List<Object>> queryResult = new ArrayList<List<Object>>(2);
		List<String[]> expectedResult = new ArrayList<String[]>(2);
		String string1 = "string1";
		String string2 = "string2";
		int int1 = 1;
		int int2 = 2;
		Date date1 = new Date(115,0,1);
		Date date2 = new Date(115,1,2);
				
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
		
		assertArrayEquals(expectedResult.get(0),sqlSourceHelper.getAllRows(queryResult).get(0));
		assertArrayEquals(expectedResult.get(1),sqlSourceHelper.getAllRows(queryResult).get(1));
	}


	@Test
	public void checkStatusFileCorrectlyCreated() {

		SQLSourceHelper sqlSourceHelper = new SQLSourceHelper(context);
		//sqlSourceHelper.setCurrentIndex(10);

		sqlSourceHelper.updateStatusFile();

		File file = new File("/tmp/flume/statusFileName.txt");
		assertEquals(true, file.exists());
		if (file.exists()){
			file.delete();
			file.getParentFile().delete();
		}

	}

	@Test
	public void checkStatusFileCorrectlyUpdated() throws Exception {

		File file = File.createTempFile("statusFileName", ".txt");

		when(context.getString("status.file.path", "/var/lib/flume")).thenReturn(file.getParent());
		when(context.getString("status.file.name")).thenReturn(file.getName());

		SQLSourceHelper sqlSourceHelper = new SQLSourceHelper(context);
		sqlSourceHelper.setCurrentIndex(10);

		sqlSourceHelper.updateStatusFile();

		SQLSourceHelper sqlSourceHelper2 = new SQLSourceHelper(context);
		assertEquals(10L, sqlSourceHelper2.getCurrentIndex());
	}
	
	@Test
	public void getConnectionURL() {
		
		SQLSourceHelper sqlSourceHelper = new SQLSourceHelper(context);
		assertEquals("jdbc:mysql://host:3306/database", sqlSourceHelper.getConnectionURL());
	}
	
	@SuppressWarnings("unused")
	@Test
	public void createDirectory() {
		
		SQLSourceHelper sqlSourceHelper = new SQLSourceHelper(context);
		File file = new File("/tmp/flume");
		assertEquals(true, file.exists());
		assertEquals(true, file.isDirectory());
		if (file.exists()){
			file.delete();
		}
	}
	
	@Test
	public void getCurrentIndex() {
		SQLSourceHelper sqlSourceHelper = new SQLSourceHelper(context);
		assertEquals(0,sqlSourceHelper.getCurrentIndex());
	}
	
	@Test
	public void setCurrentIndex() {
		SQLSourceHelper sqlSourceHelper = new SQLSourceHelper(context);
		sqlSourceHelper.setCurrentIndex(10);
		assertEquals(10,sqlSourceHelper.getCurrentIndex());
	}
	
	@Test
	public void getUser() {
		SQLSourceHelper sqlSourceHelper = new SQLSourceHelper(context);
		assertEquals("user",sqlSourceHelper.getUser());
	}
	
	@Test
	public void getPassword() {
		SQLSourceHelper sqlSourceHelper = new SQLSourceHelper(context);
		assertEquals("password",sqlSourceHelper.getPassword());
	}
	
	@Test
	public void getRunQueryDelay() {
		SQLSourceHelper sqlSourceHelper = new SQLSourceHelper(context);
		assertEquals(10000,sqlSourceHelper.getRunQueryDelay());
	}
	
	@Test
	public void getBatchSize() {
		SQLSourceHelper sqlSourceHelper = new SQLSourceHelper(context);
		assertEquals(100,sqlSourceHelper.getBatchSize());
	}
	
	@Test
	public void getQuery() {
		SQLSourceHelper sqlSourceHelper = new SQLSourceHelper(context);
		assertEquals("SELECT * FROM table",sqlSourceHelper.getQuery());
	}
	
	@Test
	public void getCustomQuery() {
		when(context.getString("custom.query")).thenReturn("SELECT column FROM table");
		SQLSourceHelper sqlSourceHelper = new SQLSourceHelper(context);
		assertEquals("SELECT column FROM table",sqlSourceHelper.getQuery());
	}
	
	@After
	public void deleteDirectory(){
		File file = new File("/tmp/flume");
		if (file.exists()){
			file.delete();
		}
	}
}
