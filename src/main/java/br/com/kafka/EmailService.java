package br.com.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.UUID;

public class EmailService {

    public static void main(String[] args) {
        var emailService = new EmailService();
        try (var service = new KafkaService(
                EmailService.class.getSimpleName(),
                EmailService.class.getSimpleName() + "_" + UUID.randomUUID().toString(),
                "ECOMMERCE_SEND_EMAIL",
                emailService::parse,
                Email.class,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
        )) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Email> record) {
        System.out.println("------------------------------------------");
        System.out.println("Sending email:");
        System.out.println("Key: " + record.key());
        System.out.println("Value: " + record.value());
        System.out.println("Partition: " + record.partition());
        System.out.println("Offset: " + record.offset());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // ignoring
            e.printStackTrace();
        }
        System.out.println("Email sent");
        System.out.println("------------------------------------------");
    }
}
