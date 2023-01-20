package com.max.datasource;


import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;


@Slf4j
@Getter
public abstract class BaseDatabaseManager {

    private final String dbUrl;
    private Connection connection = null;

    BaseDatabaseManager(String dbUrl) {
        this.dbUrl = dbUrl;
    }

    public void createConnection() throws SQLException {
        log.info("Start DB ...............");
        connection = DriverManager.getConnection(dbUrl);
    };

    public void close() throws SQLException {
        log.info("Stop DB .................");

        if (connection != null) {
            connection.close();
        }
    };


}
