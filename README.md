flume-ng-sql-source
================

This project is used for [flume-ng](https://github.com/apache/flume) to communicate with sql databases
Currently only MySQL is supported

Configuration of SQL Source:
----------

agent.sources.sql-source.type = org.apache.flume.source.sql.SQLSource
agent.sources.sql-source.connection.url = jdbc:mysql://mysql/personas
agent.sources.sql-source.user = mvalle
agent.sources.sql-source.password = mvalle
agent.sources.sql-source.table = personas
agent.sources.sql-source.columns.to.select = *
agent.sources.sql-source.incremental.column.name = id
agent.sources.sql-source.incremental.value = 0
agent.sources.sql-source.run.query.delay=1000

Thanks!!

I used flume-ng-kafka to guide me (https://github.com/baniuyao/flume-ng-kafka-source.git).
Thanks to [Frank Yao](https://github.com/baniuyao).

