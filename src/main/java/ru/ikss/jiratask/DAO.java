package ru.ikss.jiratask;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum DAO {
    I;

    private final Logger log = LoggerFactory.getLogger(DAO.class);
    public final String queryRecalcData = "select recalcData()";

    private final Properties properties;
    private final String jdbcUrl;

    DAO() {
        String driverName = "org.postgresql.Driver";
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            log.error("DAO initialization error", e);
            System.exit(-1);
        }
        String dbHost = Config.getInstance().getValue("dbHost");
        String dbName = Config.getInstance().getValue("dbName", "tasks");
        String dbUser = Config.getInstance().getValue("dbUser");
        String dbPassword = Config.getInstance().getValue("dbPassword");
        jdbcUrl = "jdbc:postgresql://" + dbHost + "/" + dbName;
        properties = new Properties();
        properties.setProperty("user", dbUser);
        properties.setProperty("password", dbPassword);
        properties.setProperty("autoReconnect", "true");
        DriverManager.setLoginTimeout(3);
    }

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, properties);
    }

    public void recalcData() {
        try (Statement st = getConnection().createStatement()) {
            log.trace("Start recalc");
            st.execute(queryRecalcData);
        } catch (Throwable e) {
            log.error("Error on recalc data", e);
        }
    }

    public DateTime getTime(String query) {
        DateTime lastTime = new DateTime(0);
        try (Statement st = getConnection().createStatement(); ResultSet rs = st.executeQuery(query)) {
            if (rs.next())
                lastTime = new DateTime(rs.getTimestamp(1));
        } catch (SQLException e) {
            log.error("Error on getting date time with query {}", query, e);
        }
        return lastTime;
    }
}
