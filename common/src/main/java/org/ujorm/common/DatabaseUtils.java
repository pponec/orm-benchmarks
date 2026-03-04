package org.ujorm.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/** Database utility for H2 connections */
public class DatabaseUtils {

    /** Provides an in-memory H2 database connection */
    public static Connection getConnection() throws SQLException {
        var url = "jdbc:h2:mem:benchmark;DB_CLOSE_DELAY=-1";
        return DriverManager.getConnection(url, "sa", "");
    }
}