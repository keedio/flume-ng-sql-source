flume-ng-sql-source
================

This project is used for [flume-ng](https://github.com/apache/flume) to communicate with sql databases

Current sql database engines supported
-------------------------------

- MySQL
- PostgreSQL
- SQLite
- Microsoft SQLServer
- Oracle



Compilation and packaging
----------
```
  $ mvn package
```

Deployment
----------

Copy flume-ng-sql-source-0.8.jar in target folder into flume plugins dir folder
```
  $ mkdir -p $FLUME_HOME/plugins.d/sql-source/lib $FLUME_HOME/plugins.d/sql-source/lib
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
##### Oracle
TODO
##### Derby
TODO
##### Microsoft SQLServer
Download the official Microsoft 4.1 Sql Server jdbc driver and copy in libext flume plugins directory:  
Download URL: https://www.microsoft.com/es-es/download/details.aspx?id=11774  
```
$ tar xzf sqljdbc_4.1.5605.100_enu.tar.gz
$ cp sqljdbc_4.1/enu/sqljdbc41.jar $FLUME_HOME/plugins.d/lib/sql-source/libext
```
##### DB2
TODO
##### Sysbase IQ
TODO

Configuration of SQL Source:
----------
Mandatory properties in <b>bold</b>

| Property Name | Default | Description |
| ----------------------- | :-----: | :---------- |
| <b>type</b> | - | The component type name, needs to be org.keedio.flume.source.SQLSource  |
| <b>connection.url</b> | - | Url to connect with the remote Database |
| <b>user</b> | - | Username to connect with the database |
| <b>password</b> | - | Password to connect with the database |
| <b>table</b> | - | Table to export data |
| <b>incremental.colum.name</b> | - | Autoincremental column name |
| <b>status.file.name</b> | - | Local file name to save last row number readed |
| status.file.path | /var/lib/flume | Path to save the status file |
| incremental.value | 0 | Start value to import data |
| columns.to.select | * | Wich colums of the table will be selected |
| run.query.delay | 10000 | ms to wait between run queries |
| batch.size| 100 | Batch size to send events to flume channel |
| max.rows | 10000| Max rows to import per query |
| custom.query | - | Custom query to force a special request to the DB, be carefull. Check below explanation of this property. |

Custom Query
-------------
A custom query is supported to bring the possibility of use entire SQL languaje. This is powerfull, but risky, be carefull with the custom queries.
To use is needed an special character '@' to indicate where the incremental value should be.
Example:
```
agent.sources.sql-source.custom.query = SELECT field1,field2 FROM table1 WHERE field1='test' AND @ ORDER BY field2;
```
Will run the query:  
```SELECT field1,field2 FROM table1 WHERE field1='test' AND incrementalColumn > 0 ORDER BY field2;```

Configuration example
--------------------

```properties

agent.sources = sql-source
agent.sources.sql-source.type = org.keedio.flume.source.SQLSource  

# URL to connect to database (currently only mysql is supported)
agent.sources.sql-source.connection.url = jdbc:mysql://host:port/database

# Database connection properties
agent.sources.sql-source.user = username  
agent.sources.sql-source.password = userpassword  
agent.sources.sql-source.table = table  
agent.sources.sql-source.database = database

# Columns to import to kafka (default * import entire row)
agent.sources.sql-source.columns.to.select = *  

# Increment column properties 
agent.sources.sql-source.incremental.column.name = id  
# Increment value is from you want to start taking data from tables (0 will import entire table)
agent.sources.sql-source.incremental.value = 0  

# Query delay, each configured milisecond the query will be sent
agent.sources.sql-source.run.query.delay=10000 

# Status file is used to save last readed row
agent.sources.sql-source.status.file.path = /var/lib/flume
agent.sources.sql-source.status.file.name = sql-source.status

# Custom query
agent.sources.sql-source.custom.query = SELECT * FROM table WHERE something AND @;
agent.sources.sql-source.batch.size = 1000;
agent.sources.sql-source.max.rows = 10000;


# 

```

Testing the source
---------------------
To test this source take a look to https://github.com/keedio/flume-stress-test-tools

Thanks!!

I used flume-ng-kafka to guide me (https://github.com/baniuyao/flume-ng-kafka-source.git).
Thanks to [Frank Yao](https://github.com/baniuyao).
