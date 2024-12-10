package repository;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class KafkaTopicManager {
    private final AdminClient adminClient;

    public KafkaTopicManager(Properties kafkaConfig) {
        this.adminClient = AdminClient.create(kafkaConfig);
    }

    public void createTopicForEmployee(int userId) {
        String topicName = "employee_expenses_" + userId;
        NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);

        try {
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
            System.out.println("Topic created: " + topicName);
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("Error creating topic: " + topicName);
            e.printStackTrace();
        }
    }

    public void deleteTopicForEmployee(int userId) {
        String topicName = "employee_expenses_" + userId;

        try {
            adminClient.deleteTopics(Collections.singletonList(topicName)).all().get();
            System.out.println("Topic deleted: " + topicName);
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("Error deleting topic: " + topicName);
            e.printStackTrace();
        }
    }

    public Set<Integer> getExistingTopicUserIds() {
        try {
            return adminClient.listTopics().names().get().stream()
                    .filter(name -> name.startsWith("employee_expenses_"))
                    .map(name -> Integer.parseInt(name.replace("employee_expenses_", "")))
                    .collect(Collectors.toSet());
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("Error fetching existing topics.");
            e.printStackTrace();
            return Collections.emptySet();
        }
    }

    public void close() {
        adminClient.close();
    }
}
