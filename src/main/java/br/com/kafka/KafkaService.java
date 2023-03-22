package br.com.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

class KafkaService {
    private final KafkaConsumer<Object, Object> consumer;
    private final ConsumerFunction parse;

    KafkaService(final String topic, ConsumerFunction parse) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(properties());
        consumer.subscribe(Collections.singletonList(topic));
    }

    public void run() {
        while (true) {
            var records = consumer.poll(Duration.ofMillis(300));
            if(!records.isEmpty()) {
                if (!records.isEmpty()) {
                    System.out.println("Encontrei " + records.count() + " registros.");
                    for (var record : records) {
                        parse.consume(record);
                    }
                }
            }
        }
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, EmailService.class.getSimpleName());
        return properties;
    }
}
