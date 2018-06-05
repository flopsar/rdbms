package org.flopsar.rdbms.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;



public class JDBCConnector {

    public static final String[] SUPPORTED_JDBC_DRIVERS = {"org.postgresql.Driver"};



    public static Connection getConnection(String driver,String host,int port,String dbname,String user,String password) throws SQLException, ClassNotFoundException {
        Class.forName(driver);
        String url = String.format("jdbc:postgresql://%s:%d/%s",host,port,dbname);
        Properties props = new Properties();
        props.setProperty("user",user);
        props.setProperty("password",password);
        Connection jdbc = DriverManager.getConnection(url, props);
        jdbc.setAutoCommit(false);
        return jdbc;
    }




}
