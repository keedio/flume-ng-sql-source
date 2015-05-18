package org.keedio.flume.source;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.flume.Context;
import org.apache.flume.conf.ConfigurationException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
* @author Marcelo Valle https://github.com/mvalleavila
*/

@RunWith(PowerMockRunner.class)
public class SQLSourceUtilsTest {

	ResultSet rs = mock(ResultSet.class);
	Context context = mock(Context.class);

	@Before
	public void setup() {

		when(context.getString("status.file.name")).thenReturn(
				"statusFileName.txt");
		when(context.getString("connection.url")).thenReturn("connectionUrl");
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
		new SQLSourceUtils(context);
	}

	@Test(expected = ConfigurationException.class)
	public void connectionURLNotSet() {
		when(context.getString("connection.url")).thenReturn(null);
		new SQLSourceUtils(context);
	}

	@Test(expected = ConfigurationException.class)
	public void tableNotSet() {
		when(context.getString("table")).thenReturn(null);
		new SQLSourceUtils(context);
	}

	@Test(expected = ConfigurationException.class)
	public void incrementalColumnNameNotSet() {
		when(context.getString("incremental.column.name")).thenReturn(null);
		new SQLSourceUtils(context);
	}

	@Test(expected = ConfigurationException.class)
	public void userNotSet() {
		when(context.getString("user")).thenReturn(null);
		new SQLSourceUtils(context);
	}

	@Test(expected = ConfigurationException.class)
	public void passwordNotSet() {
		when(context.getString("password")).thenReturn(null);
		new SQLSourceUtils(context);
	}

	@Test
	public void checkDriverParserFromURL() {

		when(context.getString("connection.url")).thenReturn(
				"jdbc:driverName://host:3306/database");

		SQLSourceUtils sqlSourceUtils = new SQLSourceUtils(context);
		assertEquals("Sql driver name not expected", "driverName", sqlSourceUtils.getDriverName());
	}

	@Test
	public void checkNotCreatedDirectory() throws Exception {

		SQLSourceUtils sqlSourceUtils = new SQLSourceUtils(context);
		SQLSourceUtils sqlSourceUtilsSpy = PowerMockito.spy(sqlSourceUtils);

		PowerMockito.verifyPrivate(sqlSourceUtilsSpy, Mockito.times(1)).invoke("createDirectory");
	}

	@Test
	public void updateStatusFileWithEmptyResultSet() throws Exception {

		try {
			SQLSourceUtils sqlSourceUtils = new SQLSourceUtils(context);
			SQLSourceUtils sqlSourceUtilsSpy = PowerMockito.spy(sqlSourceUtils);

			when(rs.isBeforeFirst()).thenReturn(false);

			sqlSourceUtils.updateStatusFile(rs);
			assertEquals("Incremental value expected: 0", 0L, sqlSourceUtils.getIncrementalValue());

			PowerMockito.verifyPrivate(sqlSourceUtilsSpy, Mockito.never()).invoke("writeStatusFile");

		} catch (NumberFormatException | SQLException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void checkStatusFileCorrectlyCreated() {

		try { 

			SQLSourceUtils sqlSourceUtils = new SQLSourceUtils(context);
			when(rs.getLong(sqlSourceUtils.getIncrementalColumnName())).thenReturn(10L);
	
			when(rs.isBeforeFirst()).thenReturn(true);
	
			sqlSourceUtils.updateStatusFile(rs);
	
			File file = new File("/tmp/flume/statusFileName.txt");
			assertEquals(true, file.exists());
			if (file.exists()){
				file.delete();
				file.getParentFile().delete();
			}
			
		}catch (SQLException e){
			e.printStackTrace();
		}
	}

	@Test
	public void checkStatusFileCorrectlyUpdated() throws Exception {

		File file = File.createTempFile("statusFileName", ".txt");

		when(context.getString("status.file.path", "/var/lib/flume")).thenReturn(file.getParent());


		when(context.getString("status.file.name")).thenReturn(file.getName());

		SQLSourceUtils sqlSourceUtils = new SQLSourceUtils(context);
		when(rs.getLong(sqlSourceUtils.getIncrementalColumnName())).thenReturn(10L);

		when(rs.isBeforeFirst()).thenReturn(true);

		sqlSourceUtils.updateStatusFile(rs);

		SQLSourceUtils sqlSourceUtils2 = new SQLSourceUtils(context);
		assertEquals(10L, sqlSourceUtils2.getIncrementalValue());
		;
	}

}
