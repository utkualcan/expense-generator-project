package service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import config.KafkaConfig;
import model.Expense;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);
    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper;

    public KafkaProducerService() {
        Properties props = KafkaConfig.getProducerConfig();
        this.producer = new KafkaProducer<>(props);
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    public void sendExpense(Expense expense) {
        try {
            String topicName = "employee_expenses_" + expense.getUserId();
            String jsonExpense = objectMapper.writeValueAsString(expense);

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topicName, String.valueOf(expense.getUserId()), jsonExpense);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Error sending expense: {}", exception.getMessage());
                } else {
                    logger.info("Expense sent to topic: {}, partition: {}, offset: {}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                }
            });
        } catch (Exception e) {
            logger.error("Error in sendExpense method: {}", e.getMessage());
        }
    }

    public void close() {
        producer.close();
    }
}