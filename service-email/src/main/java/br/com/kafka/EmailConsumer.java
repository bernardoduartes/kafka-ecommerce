package br.com.kafka;

import br.com.kafka.consumer.KafkaConsumer;
import br.com.kafka.model.EmailDTO;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class EmailConsumer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var emailService = new EmailConsumer();
        try (var consumer = new KafkaConsumer(
                EmailConsumer.class.getSimpleName(),
                EmailConsumer.class.getSimpleName() + "_" + UUID.randomUUID().toString(),
                "ECOMMERCE_SEND_EMAIL",
                emailService::parse,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
        )) {
            consumer.run();
        }
    }

    private void parse(ConsumerRecord<String, EmailDTO> record) {
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
