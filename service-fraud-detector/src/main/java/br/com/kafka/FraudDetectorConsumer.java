package br.com.kafka;

import br.com.kafka.dto.OrderDTO;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.UUID;

public class FraudDetectorConsumer {
    public static void main(String[] args) {

        var fraudDetectorService = new FraudDetectorConsumer();
        try (var consumer = new KafkaConsumer<OrderDTO>(
                FraudDetectorConsumer.class.getSimpleName(),
                FraudDetectorConsumer.class.getSimpleName() + "_" + UUID.randomUUID(),
                "ECOMMERCE_NEW_ORDER",
                fraudDetectorService::parse,
                OrderDTO.class,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
        )) {
            consumer.run();
        }
    }
    private void parse(ConsumerRecord<String, OrderDTO> record) {
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