flume-ng-sql-source
================

This project is used for [flume-ng](https://github.com/apache/flume) to communicate with sql databases

Current sql database engines supported
-------------------------------
- After the last update the code has been integrated with hibernate, so all databases supported by this technology should work.

Compilation and packaging
----------
```
  $ mvn package
```

Deployment
----------

Copy flume-ng-sql-source-<version>.jar in target folder into flume plugins dir folder
```
  $ mkdir -p $FLUME_HOME/plugins.d/sql-source/lib $FLUME_HOME/plugins.d/sql-source/libext
  $ cp flume-ng-sql-source-0.8.jar $FLUME_HOME/plugins.d/sql-source/lib
```

### Specific installation by database engine

##### MySQL
Download the official mysql jdbc driver and copy in libext flume plugins directory:
```
$ wget https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-5.1.35.tar.gz
$ tar xzf mysql-connector-java-5.1.35.tar.gz
$ cp mysql-connector-java-5.1.35-bin.jar $FLUME_HOME/plugins.d/lib/sql-source/libext
```

##### Microsoft SQLServer
Download the official Microsoft 4.1 Sql Server jdbc driver and copy in libext flume plugins directory:  
Download URL: https://www.microsoft.com/es-es/download/details.aspx?id=11774  
```
$ tar xzf sqljdbc_4.1.5605.100_enu.tar.gz
$ cp sqljdbc_4.1/enu/sqljdbc41.jar $FLUME_HOME/plugins.d/lib/sql-source/libext
```

##### IBM DB2
Download the official IBM DB2 jdbc driver and copy in libext flume plugins directory:
Download URL: http://www-01.ibm.com/support/docview.wss?uid=swg21363866

Configuration of SQL Source:
----------
Mandatory properties in <b>bold</b>

| Property Name | Default | Description |
| ----------------------- | :-----: | :---------- |
| <b>channels</b> | - | Connected channel names |
| <b>type</b> | - | The component type name, needs to be org.keedio.flume.source.SQLSource  |
| <b>hibernate.connection.url</b> | - | Url to connect with the remote Database |
| <b>hibernate.connection.user</b> | - | Username to connect with the database |
| <b>hibernate.connection.password</b> | - | Password to connect with the database |
| <b>table</b> | - | Table to export data |
| <b>status.file.name</b> | - | Local file name to save last row number read |
| status.file.path | /var/lib/flume | Path to save the status file |
| start.from | 0 | Start value to import data |
| columns.to.select | * | Which colums of the table will be selected |
| run.query.delay | 10000 | ms to wait between run queries |
| batch.size| 100 | Batch size to send events to flume channel |
| max.rows | 10000| Max rows to import per query |
| read.only | false| Sets read only session with DDBB |
| custom.query | - | Custom query to force a special request to the DB, be carefull. Check below explanation of this property. |
| hibernate.connection.driver_class | -| Driver class to use by hibernate, if not specified the framework will auto asign one |
| hibernate.dialect | - | Dialect to use by hibernate, if not specified the framework will auto asign one. Check https://docs.jboss.org/hibernate/orm/4.3/manual/en-US/html/ch03.html#configuration-optional-dialects for a complete list of available dialects |
| hibernate.connection.provider_class | - | Set to org.hibernate.connection.C3P0ConnectionProvider to use C3P0 connection pool (recommended for production) |
| hibernate.c3p0.min_size | - | Min connection pool size |
| hibernate.c3p0.max_size | - | Max connection pool size |

Standard Query
-------------
If no custom query is set, ```SELECT <columns.to.select> FROM <table>``` will be executed each ```run.query.delay``` milliseconds configured

Custom Query
-------------
A custom query is supported to bring the possibility of using the entire SQL language. This is powerful, but risky, be careful with the custom queries used.  

To avoid row export repetitions use the $@$ special character in WHERE clause, to incrementaly export not processed rows and the new ones inserted.

IMPORTANT: For proper operation of Custom Query ensure that incremental field will be returned in the first position of the Query result.

Example:
```
agent.sources.sql-source.custom.query = SELECT incrementalField,field2 FROM table1 WHERE incrementalField > $@$ 
```

Configuration example
--------------------

```properties
# For each one of the sources, the type is defined
agent.sources.sqlSource.type = org.keedio.flume.source.SQLSource

agent.sources.sqlSource.hibernate.connection.url = jdbc:db2://192.168.56.70:50000/sample

# Hibernate Database connection properties
agent.sources.sqlSource.hibernate.connection.user = db2inst1
agent.sources.sqlSource.hibernate.connection.password = db2inst1
agent.sources.sqlSource.hibernate.connection.autocommit = true
agent.sources.sqlSource.hibernate.dialect = org.hibernate.dialect.DB2Dialect
agent.sources.sqlSource.hibernate.connection.driver_class = com.ibm.db2.jcc.DB2Driver

#agent.sources.sqlSource.table = employee1

# Columns to import to kafka (default * import entire row)
#agent.sources.sqlSource.columns.to.select = *

# Query delay, each configured milisecond the query will be sent
agent.sources.sqlSource.run.query.delay=10000

# Status file is used to save last readed row
agent.sources.sqlSource.status.file.path = /var/log/flume
agent.sources.sqlSource.status.file.name = sqlSource.status

# Custom query
agent.sources.sqlSource.start.from = 19700101000000000000
agent.sources.sqlSource.custom.query = SELECT * FROM (select DECIMAL(test) * 1000000 AS INCREMENTAL, EMPLOYEE1.* from employee1 UNION select DECIMAL(test) * 1000000 AS INCREMENTAL, EMPLOYEE2.* from employee2) WHERE INCREMENTAL > $@$ ORDER BY INCREMENTAL ASC

agent.sources.sqlSource.batch.size = 1000
agent.sources.sqlSource.max.rows = 1000

agent.sources.sqlSource.hibernate.connection.provider_class = org.hibernate.connection.C3P0ConnectionProvider
agent.sources.sqlSource.hibernate.c3p0.min_size=1
agent.sources.sqlSource.hibernate.c3p0.max_size=10

# The channel can be defined as follows.
agent.sources.sqlSource.channels = memoryChannel
```

Known Issues
---------
An issue with Java SQL Types and Hibernate Types could appear Using SQL Server databases and SQL Server Dialect coming with Hibernate.  
  
Something like:
```
org.hibernate.MappingException: No Dialect mapping for JDBC type: -15
```

Use ```org.keedio.flume.source.SQLServerCustomDialect``` in flume configuration file to solve this problem.

Special thanks
---------------

I used flume-ng-kafka to guide me (https://github.com/baniuyao/flume-ng-kafka-source.git).
Thanks to [Frank Yao](https://github.com/baniuyao).
