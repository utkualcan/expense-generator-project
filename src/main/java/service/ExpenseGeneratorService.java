package service;

import com.datastax.oss.driver.api.core.CqlSession;
import config.KafkaConfig;
import model.Employee;
import model.Expense;
import repository.EmployeeRepository;
import repository.KafkaTopicManager;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
                .forEach(userId -> {
                    topicManager.deleteTopicForEmployee(userId);
                    deleteEmployeeFromCassandra(userId);
                });
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
        return employees.stream()
                .map(Employee::getUserId)
                .collect(Collectors.toSet());
    }

    private void deleteEmployeeFromCassandra(int userId) {
        try (CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
                .withAuthCredentials("spark_user", "kurgualcan76")
                .withKeyspace("expenses_ks")
                .withLocalDatacenter("datacenter1")
                .build()) {

            String deleteQuery = "DELETE FROM expenses WHERE user_id = ?";
            session.execute(deleteQuery, userId);
            System.out.println("Cassandra'dan silinen çalışan id: " + userId);
        } catch (Exception e) {
            System.err.println("Cassandra'dan silerken hata oluştu: " + e.getMessage());
        }
    }

}
