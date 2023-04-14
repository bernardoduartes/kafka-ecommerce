package br.com.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

class KafkaConsumer<T> implements Closeable {
    private final org.apache.kafka.clients.consumer.KafkaConsumer<String, T> consumer;
    private final ConsumerFunction parse;

    KafkaConsumer(final String groupIdConfig, final String clientIdConfig, final String topic, ConsumerFunction parse, Class<T> type, Map<String,String> properties) {
        this(groupIdConfig, clientIdConfig, parse, type, properties);
        consumer.subscribe(Collections.singletonList(topic));
    }

    KafkaConsumer(final String groupIdConfig, final String clientIdConfig, final Pattern topic, ConsumerFunction parse, Class<T> type, Map<String,String> properties) {
        this(groupIdConfig, clientIdConfig, parse, type, properties);
        consumer.subscribe(topic);
    }

    private KafkaConsumer(final String groupIdConfig, final String clientIdConfig, ConsumerFunction parse, Class<T> type, Map<String,String> properties) {
        this.parse = parse;
        this.consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(this.properties(groupIdConfig, clientIdConfig, type, properties));
    }

    void run() {
        while (true) {
            var records = consumer.poll(Duration.ofMillis(300));
            if (!records.isEmpty()) {
                if (!records.isEmpty()) {
                    System.out.println("Encontrei " + records.count() + " registros.");
                    for (var record : records) {
                        try {
                            parse.consume(record);
                        } catch (ExecutionException e) {
                            throw new RuntimeException(e);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }
    }

    private Properties properties(String groupIdConfig, String clientIdConfig, Class<T> type, Map<String,String> overrideProperties) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupIdConfig);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientIdConfig);
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.putAll(overrideProperties);
        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
