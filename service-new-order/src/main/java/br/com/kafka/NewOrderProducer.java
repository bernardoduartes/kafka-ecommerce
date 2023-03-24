package br.com.kafka;

import br.com.kafka.dto.EmailDTO;
import br.com.kafka.dto.OrderDTO;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
       try(var orderProducer = new KafkaProducer<OrderDTO>()) {
           try(var emailProducer = new KafkaProducer<EmailDTO>()) {
               for (var i = 0; i < 10; i++) {

                   var userId = UUID.randomUUID().toString();
                   var orderId = UUID.randomUUID().toString();
                   var amount = new BigDecimal(Math.random() * 5000 + 1);

                   var order = new OrderDTO(userId, orderId, amount);
                   orderProducer.send("ECOMMERCE_NEW_ORDER", userId, order);

                   var email = new EmailDTO("Email de compra", "Obrigado pela compra! Estamos processando seu pedido!");
                   emailProducer.send("ECOMMERCE_SEND_EMAIL", userId, email);
               }
           }
       }

    }
}