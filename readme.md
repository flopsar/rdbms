# Flopsar Database to RDBMS converter

This is a simple application, which allows to transfer data from the Flopsar database to relational database.
Currently, there is only one database supported out of the box.

In order to transfer data to a relational database, first you need to create a database and a schema.
The available schemas can be found in `resources/<database>/ddl.sql` file.


## PostgreSQL
```
$ psql -h <host> -p <port> -U <usernam> -f ddl.sql
$ psql -h <host> -p <port> -U <usernam> -f dml.sql
```



## How to add support for another database

There are a few steps required to add support for another database. Follow the instructions below:

1. Create your own database schema file. See [`resources/postgresql/ddl.sql`](https://github.com/dsendkowski/rdbms/blob/master/src/main/resources/postgresql/ddl.sql) as an example.
2. Create a class that implements [`org.flopsar.rdbms.jdbc.DML`](https://github.com/dsendkowski/rdbms/blob/master/src/main/java/org/flopsar/rdbms/jdbc/DML.java) interface. See [`org.flopsar.rdbms.jdbc.PostgresDML`](https://github.com/dsendkowski/rdbms/blob/master/src/main/java/org/flopsar/rdbms/jdbc/PostgresDML.java) as an example.
3. Add a new element to the [`SUPPORTED_JDBC_DRIVERS`](https://github.com/dsendkowski/rdbms/blob/396f9949a0535b0523f571612e13f15d07fdc117/src/main/java/org/flopsar/rdbms/jdbc/JDBCConnector.java#L12) array in [`org.flopsar.rdbms.jdbc.JDBCConnector`](https://github.com/dsendkowski/rdbms/blob/master/src/main/java/org/flopsar/rdbms/jdbc/JDBCConnector.java) class.
4. Add a dependency (JDBC jar file) to the Gradle build script.








