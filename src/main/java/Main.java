import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import service.ExpenseGeneratorService;
import service.KafkaProducerService;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class Main {

    public static void main(String[] args) {
        KafkaProducerService kafkaProducerService = new KafkaProducerService();

        ExpenseGeneratorService expenseGeneratorService = new ExpenseGeneratorService(kafkaProducerService);

        System.out.println("Başlatılıyor...");
        expenseGeneratorService.initializeTopics();
        expenseGeneratorService.startExpenseGeneration();

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "expense-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);

        Set<Integer> userIds = expenseGeneratorService.getEmployeeUserIds();
        Set<String> topics = userIds.stream()
                .map(userId -> "employee_expenses_" + userId)
                .collect(Collectors.toSet());

        consumer.subscribe(topics);

        final CqlSession[] session = new CqlSession[1];
        try {
            session[0] = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
                    .withAuthCredentials("spark_user", "kurgualcan76")
                    .withKeyspace("expenses_ks")
                    .withLocalDatacenter("datacenter1")
                    .build();

            System.out.println("Cassandra bağlantısı başarılı!");
        } catch (Exception e) {
            System.err.println("Cassandra bağlantısı hatası: " + e.getMessage());
            return;
        }

        while (true) {

            Set<Integer> userIdsControl = expenseGeneratorService.getEmployeeUserIds();
            Set<String> topicsControl = userIdsControl.stream()
                    .map(userId -> "employee_expenses_" + userId)
                    .collect(Collectors.toSet());

            if (topicsControl.size() != topics.size()) {
                topics = topicsControl;
                expenseGeneratorService.updateTopics();
                consumer.unsubscribe();
                consumer.subscribe(topics);
            }

            consumer.poll(Duration.ofMillis(1000)).forEach(record -> {
                String expenseData = record.value();
                System.out.println("Veri alındı: " + expenseData);

                String[] values = expenseData.split(",", -1);

                if (values.length == 6) {
                    try {
                        String empnoString = values[0].split(":")[1].trim();
                        int empno = Integer.parseInt(empnoString);

                        String epochTimeString = values[1].split(":")[1].trim().replace("\"", "");
                        double epochTime = Double.parseDouble(epochTimeString);
                        long epochSeconds = (long) epochTime;
                        int nanoAdjustment = (int) ((epochTime - epochSeconds) * 1_000_000_000);
                        Instant dateTime = Instant.ofEpochSecond(epochSeconds, nanoAdjustment);

                        String description = values[3].split(":")[1].trim();
                        String type = values[4].split(":")[1].trim();

                        int count = 0;
                        double payment = 0.0;

                        try {
                            String countString = values[2].split(":")[1].trim();
                            count = (int) Double.parseDouble(countString);
                        } catch (NumberFormatException e) {
                            System.err.println("Hata: count değeri sayıya dönüştürülemedi: " + values[2]);
                        }

                        try {
                            String paymentString = values[5].split(":")[1].trim().replace("}", "");
                            payment = Double.parseDouble(paymentString);
                        } catch (NumberFormatException e) {
                            System.err.println("Hata: payment değeri sayıya dönüştürülemedi: " + values[5]);
                        }

                        String query = "INSERT INTO expenses (user_id, date_time, count, description, expense_type, payment) VALUES (?, ?, ?, ?, ?, ?)";
                        session[0].execute(query, empno, dateTime, count, description, type, payment);

                        System.out.println("Veri Cassandra'ya yazıldı: " + expenseData);

                    } catch (Exception e) {
                        System.err.println("Veri işlenirken hata oluştu: " + e.getMessage());
                    }
                } else {
                    System.err.println("Invalid record: Expected 6 fields but received " + values.length + " - " + expenseData);
                }
            });

        }
    }
}
