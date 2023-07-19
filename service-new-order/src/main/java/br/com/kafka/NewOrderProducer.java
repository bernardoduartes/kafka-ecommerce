package br.com.kafka;

import br.com.kafka.model.Order;
import br.com.kafka.producer.KafkaProducer;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var orderProducer = new KafkaProducer<Order>()) {
            var email = Math.random() + "@email.com";
            for (var i = 0; i < 10; i++) {
                var orderId = UUID.randomUUID().toString();
                var amount = new BigDecimal(Math.random() * 5000 + 1);
                var id = new CurrelationId(NewOrderProducer.class.getSimpleName());
                var order = new Order(orderId, amount, email);
                orderProducer.send(
                        "ECOMMERCE_NEW_ORDER",
                        email,
                        id,
                        order
                );
            }

        }

    }
}