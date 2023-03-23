package br.com.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class EmailService {

    public static void main(String [] args){
        var emailService = new EmailService();
        var service = new KafkaService(
                EmailService.class.getSimpleName(),
                EmailService.class.getSimpleName() + "_" + UUID.randomUUID().toString(),
                "ECOMMERCE_SEND_EMAIL",
                emailService::parse
        );
        service.run();
    }

    private void parse(ConsumerRecord<Object, Object> record) {
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
