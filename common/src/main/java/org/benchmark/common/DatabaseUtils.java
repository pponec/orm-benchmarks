package org.benchmark.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/** Database utility for H2 connections */
public class DatabaseUtils {

    private static boolean initialized = false;

    private static final String DDL = """
            CREATE TABLE city
               ( id BIGINT AUTO_INCREMENT PRIMARY KEY
               , name VARCHAR(50) NOT NULL
               , country_code VARCHAR(2) NOT NULL
               , latitude DECIMAL(10, 8) NOT NULL
               , longitude DECIMAL(11, 8) NOT NULL
               , created_at TIMESTAMP NOT NULL
               , updated_at TIMESTAMP NOT NULL
               , created_by VARCHAR(50) NOT NULL
               , updated_by VARCHAR(50) NOT NULL
               );
            CREATE TABLE employee
               ( id BIGINT AUTO_INCREMENT PRIMARY KEY
               , name VARCHAR(50) NOT NULL
               , superior_id BIGINT NULL
               , city_id BIGINT NOT NULL
               , contract_day DATE NULL
               , is_active BOOLEAN DEFAULT true
               , email VARCHAR(100) NULL
               , phone VARCHAR(20) NULL
               , salary DECIMAL(10, 2) NULL
               , department VARCHAR(50) NULL
               , created_at TIMESTAMP NOT NULL
               , updated_at TIMESTAMP NOT NULL
               , created_by VARCHAR(50) NOT NULL
               , updated_by VARCHAR(50) NOT NULL
               );
            ALTER TABLE employee ADD CONSTRAINT fk_employee_superior_id__id
                  FOREIGN KEY (superior_id) REFERENCES employee(id)
                  ON DELETE RESTRICT ON UPDATE RESTRICT;
            ALTER TABLE employee ADD CONSTRAINT fk_employee_city_id__id
                  FOREIGN KEY (city_id) REFERENCES city(id)
                  ON DELETE RESTRICT ON UPDATE RESTRICT;
            
            INSERT INTO city (name, country_code, latitude, longitude, created_at, updated_at, created_by, updated_by)
            VALUES ('Prague', 'CZ', 50.0755, 14.4378, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'System', 'System');
            """;

    /** Provides an initialized H2 database connection */
    public static Connection getConnection() throws SQLException {
        var url = "jdbc:h2:mem:benchmark;DB_CLOSE_DELAY=-1";
        var result = DriverManager.getConnection(url, "sa", "");
        result.setAutoCommit(false);

        if (!initialized) {
            synchronized (DatabaseUtils.class) {
                if (!initialized) {
                    try (var statement = result.createStatement()) {
                        statement.execute(DDL);
                    }
                    result.commit();
                    initialized = true;
                }
            }
        }
        return result;
    }
}