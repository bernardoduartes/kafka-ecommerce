package br.com.kafka;


import br.com.kafka.consumer.ConsumerService;
import br.com.kafka.consumer.KafkaConsumer;
import br.com.kafka.consumer.ServiceRunner;
import br.com.kafka.producer.KafkaProducer;
import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;


public class FraudDetectorConsumer implements ConsumerService<Order> {

    private final KafkaProducer<Order> orderProducer = new KafkaProducer<>();

    private final LocalDatabase database;

    FraudDetectorConsumer() throws SQLException {
        this.database = new LocalDatabase("frauds_database");
        this.database.createIfNotExists("CREATE TABLE IF NOT EXISTS Orders (" +
                "uuid varchar(200) primary key," +
                "is_fraud boolean)");
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
//                 Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, FraudDetectorGsonDeserializer.class.getName())
        new ServiceRunner<>(FraudDetectorConsumer::new).start(1);
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException, SQLException {

        System.out.println("------------------------------------------");
        System.out.println("Processing new order, checking for fraud");
        System.out.println("Key: " + record.key());
        System.out.println("Value: " + record.value());
        System.out.println("Partition: " + record.partition());
        System.out.println("Offset: " + record.offset());

        var message = record.value();
        var order = message.getPayload();

        if(wasProcessed(order.getOrderId())) {
            System.out.println("Order " + order.getOrderId() + " was already processed!");
        }

        if(isFraud(order.getAmount())) {
            database.update("inset into Orders (uuid, is_fraud) values (?, true)", order.getOrderId());
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

    private boolean wasProcessed(final String orderId) throws SQLException {
        var results = database.query("select uuid from Orders where uuid = ? limit 1", orderId);
        return results.next();
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return FraudDetectorConsumer.class.getSimpleName();
    }

    private boolean isFraud(final BigDecimal amount){
        return amount.compareTo(new BigDecimal("4500")) >= 0;
    }
}