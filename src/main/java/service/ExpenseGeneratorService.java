package service;

import config.KafkaConfig;
import model.Employee;
import model.Expense;
import repository.EmployeeRepository;
import repository.KafkaTopicManager;

import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ExpenseGeneratorService {
    private final EmployeeRepository employeeRepository;
    private final KafkaTopicManager topicManager;
    private final KafkaProducerService kafkaProducerService;
    private final ScheduledExecutorService scheduler;

    public ExpenseGeneratorService(KafkaProducerService kafkaProducerService) {
        this.employeeRepository = new EmployeeRepository();
        this.topicManager = new KafkaTopicManager(KafkaConfig.getProducerConfig());
        this.kafkaProducerService = kafkaProducerService;
        this.scheduler = Executors.newScheduledThreadPool(1);
    }

    public void initializeTopics() {
        List<Employee> employees = employeeRepository.getAllEmployees();
        employees.forEach(emp -> topicManager.createTopicForEmployee(emp.getUserId()));
    }

    public void updateTopics() {
        List<Employee> currentEmployees = employeeRepository.getAllEmployees();
        Set<Integer> existingTopics = topicManager.getExistingTopicUserIds();

        currentEmployees.stream()
                .filter(emp -> !existingTopics.contains(emp.getUserId()))
                .forEach(emp -> topicManager.createTopicForEmployee(emp.getUserId()));

        existingTopics.stream()
                .filter(userId -> currentEmployees.stream().noneMatch(emp -> emp.getUserId() == userId))
                .forEach(userId -> topicManager.deleteTopicForEmployee(userId));
    }

    public void startExpenseGeneration() {
        scheduler.scheduleAtFixedRate(() -> {
            updateTopics();
            List<Employee> employees = employeeRepository.getAllEmployees();
            employees.forEach(emp -> {
                Expense expense = new Expense(emp.getUserId());
                kafkaProducerService.sendExpense(expense);
            });
        }, 0, 1, TimeUnit.SECONDS);
    }

    public Set<Integer> getEmployeeUserIds() {
        List<Employee> employees = employeeRepository.getAllEmployees();
        Set<Integer> userIds = new HashSet<>();
        for (Employee employee : employees) {
            userIds.add(employee.getUserId());
        }
        return userIds;
    }
}
