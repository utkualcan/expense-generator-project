package repository;

import config.DatabaseConfig;
import model.Employee;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class EmployeeRepository {
    public List<Employee> getAllEmployees() {
        List<Employee> employees = new ArrayList<>();
        String query = "SELECT empno, ename, job, sal FROM employee";

        try (Connection conn = DatabaseConfig.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(query);
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                employees.add(new Employee(
                        rs.getInt("empno"),
                        rs.getString("ename"),
                        rs.getString("job"),
                        rs.getDouble("sal")
                ));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return employees;
    }
}