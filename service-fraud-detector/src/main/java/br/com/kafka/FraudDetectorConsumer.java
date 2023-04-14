package br.com.kafka;


import br.com.kafka.dto.OrderDTO;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class FraudDetectorConsumer {

    private final KafkaProducer<OrderDTO> orderProducer = new KafkaProducer<OrderDTO>();
    public static void main(String[] args) {

        var fraudDetectorService = new FraudDetectorConsumer();
        try (var consumer = new KafkaConsumer<OrderDTO>(
                FraudDetectorConsumer.class.getSimpleName(),
                FraudDetectorConsumer.class.getSimpleName() + "_" + UUID.randomUUID(),
                "ECOMMERCE_NEW_ORDER",
                fraudDetectorService::parse,
                OrderDTO.class,
                Map.of()
        )) {
            consumer.run();
        }
    }
    private void parse(ConsumerRecord<String, OrderDTO> record) throws ExecutionException, InterruptedException {


        System.out.println("------------------------------------------");
        System.out.println("Processing new order, checking for fraud");
        System.out.println("Key: " + record.key());
        System.out.println("Value: " + record.value());
        System.out.println("Partition: " + record.partition());
        System.out.println("Offset: " + record.offset());

        var order = record.value();
        if(isFraud(order.getAmount())) {
            System.out.println("Order not approved. Fraud : " + order);
            orderProducer.send("ECOMMERCE_ORDER_REJECTED", order.getUserId(), order);
        } else {
            System.out.println("Order approved: " + order);
            orderProducer.send("ECOMMERCE_ORDER_APPROVED", order.getUserId(), order);
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