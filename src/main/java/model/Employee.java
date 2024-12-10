package model;

public class Employee {
    private int userId;
    private String name;
    private String job;
    private double salary;

    public Employee(int userId, String name, String job, double salary) {
        this.userId = userId;
        this.name = name;
        this.job = job;
        this.salary = salary;
    }

    // Getters and setters
    public int getUserId() { return userId; }
    public String getName() { return name; }
    public String getJob() { return job; }
    public double getSalary() { return salary; }
}