package br.com.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

public class LogService {

    public static void main(String[] args) {
        var logService = new LogService();
        try (var service = new KafkaService(
                LogService.class.getSimpleName(),
                LogService.class.getSimpleName() + "_" + UUID.randomUUID().toString(),
                Pattern.compile("ECOMMERCE.*"),
                logService::parse,
                Object.class,
                Map.of()
        )) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<Object, Object> record) {
        System.out.println("------------------------------------------");
        System.out.println("LOG:" + record.topic());
        System.out.println("Key: " + record.key());
        System.out.println("Value: " + record.value());
        System.out.println("Partition: " + record.partition());
        System.out.println("Offset: " + record.offset());
        System.out.println("------------------------------------------");
    }
}
