package br.com.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class FraudDetectorService {

    public static void main(String [] args){

        var fraudDetectorService = new FraudDetectorService();
        var service = new KafkaService(
                FraudDetectorService.class.getSimpleName(),
                FraudDetectorService.class.getSimpleName() + "_" + UUID.randomUUID().toString(),
                "ECOMMERCE_NEW_ORDER",
                fraudDetectorService::parse
        );
        service.run();
    }

    private void parse(ConsumerRecord<Object, Object> record) {
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
