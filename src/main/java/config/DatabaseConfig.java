package config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ResourceBundle;

public class DatabaseConfig {
    private static final ResourceBundle config = ResourceBundle.getBundle("application");

    private static final String URL = config.getString("mysql.url");
    private static final String USER = config.getString("mysql.username");
    private static final String PASSWORD = config.getString("mysql.password");

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(URL, USER, PASSWORD);
    }
}