package br.com.kafka;

import br.com.kafka.consumer.ConsumerService;
import br.com.kafka.consumer.KafkaConsumer;
import br.com.kafka.consumer.ServiceRunner;
import br.com.kafka.producer.KafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;


public class ServiceEmailNewOrderConsumer  implements ConsumerService<Order> {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        new ServiceRunner(ServiceEmailNewOrderConsumer::new).start(1);
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {

        System.out.println("------------------------------------------");
        System.out.println("Processing new order, preparing email");
        System.out.println("Key: " + record.key());
        System.out.println("Value: " + record.value());
        System.out.println("Partition: " + record.partition());
        System.out.println("Offset: " + record.offset());

        var message = record.value();
        var order = message.getPayload();

        try (var emailProducer = new KafkaProducer<String>()) {
            var email = Math.random() + "@email.com";
            var id = new CurrelationId(ServiceEmailNewOrderConsumer.class.getSimpleName());

            var emailSubject = "Email de compra, Obrigado pela compra! Estamos processando seu pedido!";
            emailProducer.send(
                    "ECOMMERCE_SEND_EMAIL",
                    order.getEmail(),
                    message.getId().continueWith(ServiceEmailNewOrderConsumer.class.getSimpleName()),
                    emailSubject
            );
        }
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return ServiceEmailNewOrderConsumer.class.getSimpleName();
    }

    private boolean isFraud(final BigDecimal amount) {
        return amount.compareTo(new BigDecimal("4500")) >= 0;
    }
}