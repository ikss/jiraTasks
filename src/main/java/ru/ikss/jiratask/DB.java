package ru.ikss.jiratask;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DB {

    public static final String queryInsertDataSet5 = "select taskInsert(?, ?, ?, ?, ?, ?, ?, ?)";
    public static final String queryInsertDataSet10 = "select set10TaskInsert(?, ?, ?, ?, ?, ?, ?, ?, ?)";
    public static final String queryInsertDataCR = "select CRTaskInsert(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    public static final String queryInsertWorklogCR = "select CRWorkLogInsert(?, ?, ?, ?)";
    public static final String queryGetTimeSet5 = "select getLastTaskDate()";
    public static final String queryGetTimeSet10 = "select set10GetLastTaskDate()";
    public static final String queryGetTimeCR = "select CRGetLastTaskDate()";
    public static final String queryRecalcData = "select recalcData()";

    public static Connection getConnection(Properties props) throws SQLException {
        try {
            Connection conn = null;
            String dbHost = props.getProperty("dbHost");
            String dbName = props.getProperty("dbName", "tasks");
            String dbUser = props.getProperty("dbUser");
            String dbPassword = props.getProperty("dbPassword");
            String jdbcUrl = "jdbc:postgresql://" + dbHost + "/" + dbName;
            String driverName = "org.postgresql.Driver";
            Class.forName(driverName);
            Properties properties = new Properties();
            properties.setProperty("user", dbUser);
            properties.setProperty("password", dbPassword);
            properties.setProperty("autoReconnect", "true");
            DriverManager.setLoginTimeout(3);
            conn = DriverManager.getConnection(jdbcUrl, properties);

            if (conn == null) {
                throw new SQLException("Не удалось подключиться к БД");
            }
            return conn;
        } catch (ClassNotFoundException ex) {
            throw new SQLException("PGSQL driver not found");
        }
    }
}
