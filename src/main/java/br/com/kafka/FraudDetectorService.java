package br.com.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class FraudDetectorService {
    public static void main(String[] args) {

        var fraudDetectorService = new FraudDetectorService();
        try (var service = new KafkaService<Order>(
                FraudDetectorService.class.getSimpleName(),
                FraudDetectorService.class.getSimpleName() + "_" + UUID.randomUUID(),
                "ECOMMERCE_NEW_ORDER",
                fraudDetectorService::parse,
                Order.class,
                Map.of()
        )) {
            service.run();
        }
    }
    private void parse(ConsumerRecord<String, Order> record) {
        System.out.println("------------------------------------------");
        System.out.println("Processing new order, checking for fraud");
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
        System.out.println("Order processed");
        System.out.println("------------------------------------------");
    }
}