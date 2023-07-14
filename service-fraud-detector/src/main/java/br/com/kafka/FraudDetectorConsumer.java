package br.com.kafka;


import br.com.kafka.consumer.KafkaConsumer;
import br.com.kafka.producer.KafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;


public class FraudDetectorConsumer {

    private final KafkaProducer<Order> orderProducer = new KafkaProducer<>();

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        var fraudDetectorService = new FraudDetectorConsumer();
        try (var consumer = new KafkaConsumer<>(
                FraudDetectorConsumer.class.getSimpleName(),
                FraudDetectorConsumer.class.getSimpleName() + "_" + UUID.randomUUID(),
                "ECOMMERCE_NEW_ORDER",
                fraudDetectorService::parse,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, FraudDetectorGsonDeserializer.class.getName())
        )) {
            consumer.run();
        }
    }
    void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {

        System.out.println("------------------------------------------");
        System.out.println("Processing new order, checking for fraud");
        System.out.println("Key: " + record.key());
        System.out.println("Value: " + record.value());
        System.out.println("Partition: " + record.partition());
        System.out.println("Offset: " + record.offset());

        var message = record.value();
        var order = message.getPayload();
        if(isFraud(order.getAmount())) {
            System.out.println("Order not approved. Fraud : " + order);
            orderProducer.send(
                    "ECOMMERCE_ORDER_REJECTED",
                    order.getEmail(),
                    message.getId().continueWith(FraudDetectorConsumer.class.getSimpleName()),
                    order
            );
        } else {
            System.out.println("Order approved: " + order);
            orderProducer.send(
                    "ECOMMERCE_ORDER_APPROVED",
                    order.getEmail(),
                    message.getId().continueWith(FraudDetectorConsumer.class.getSimpleName()),
                    order
            );
        }

        System.out.println("Order processed");
        System.out.println("------------------------------------------");

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // ignoring
            e.printStackTrace();
        }
    }

    private boolean isFraud(final BigDecimal amount){
        return amount.compareTo(new BigDecimal("4500")) >= 0;
    }
}