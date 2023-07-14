package br.com.kafka;


import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class FraudDetectorConsumer {

    private final KafkaProducer<Order> orderProducer = new KafkaProducer<>();
    public static void main(String[] args) {

        var fraudDetectorService = new FraudDetectorConsumer();
        try (var consumer = new KafkaConsumer<Order>(
                FraudDetectorConsumer.class.getSimpleName(),
                FraudDetectorConsumer.class.getSimpleName() + "_" + UUID.randomUUID(),
                "ECOMMERCE_NEW_ORDER",
                fraudDetectorService::parse,
                Order.class,
                Map.of()
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
            orderProducer.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(), order);
        } else {
            System.out.println("Order approved: " + order);
            orderProducer.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(), order);
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