package br.com.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

class KafkaService implements Closeable {
    private final KafkaConsumer<Object, Object> consumer;
    private final ConsumerFunction parse;

    KafkaService(final String groupIdConfig, final String clientIdConfig, final String topic, ConsumerFunction parse) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(this.properties(groupIdConfig, clientIdConfig));
        consumer.subscribe(Collections.singletonList(topic));
    }

    void run() {
        while (true) {
            var records = consumer.poll(Duration.ofMillis(300));
            if (!records.isEmpty()) {
                if (!records.isEmpty()) {
                    System.out.println("Encontrei " + records.count() + " registros.");
                    for (var record : records) {
                        parse.consume(record);
                    }
                }
            }
        }
    }

    private Properties properties(final String groupIdConfig, final String clientIdConfig) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupIdConfig);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientIdConfig);
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
