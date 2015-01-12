flume-ng-sql-source
================

This project is used for [flume-ng](https://github.com/apache/flume) to communicate with sql databases

Current sql database engines supported
-------------------------------

- MySQL
- PostgreSQL
- SQLite
- Microsoft SQLServer

On development databases
------------------------
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
TODO
##### Oracle
TODO
##### Derby
TODO
##### Microsoft SQLServer
TODO
##### DB2
TODO
##### Sysbase IQ
TODO

Configuration of SQL Source:
----------

```

agent.sources = sql-source
agent.sources.sql-source.type = org.apache.flume.source.SQLSource  

# URL to connect to database (currently only mysql is supported)
agent.sources.sql-source.connection.url = jdbc:mysql://host:port/database

# Database connection properties
agent.sources.sql-source.user = username  
agent.sources.sql-source.password = userpassword  
agent.sources.sql-source.table = table  

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

```

Testing the source
---------------------
To test this source take a look to https://github.com/keedio/flume-stress-test-tools

Thanks!!

I used flume-ng-kafka to guide me (https://github.com/baniuyao/flume-ng-kafka-source.git).
Thanks to [Frank Yao](https://github.com/baniuyao).
