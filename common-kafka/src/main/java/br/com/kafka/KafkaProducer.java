package br.com.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducer<T> implements Closeable {

    private final org.apache.kafka.clients.producer.KafkaProducer<String, Message<T>> producer;

    public KafkaProducer() {
        this.producer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties());
    }

    public void send(final String topic, final String key, CurrelationId currelationId, final T payload) throws ExecutionException, InterruptedException {
        var value = new Message<>(currelationId, payload);
        var records = new ProducerRecord<>(topic, key, value);
        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("sucesso enviando " + data.topic() + ":::partition " + data.partition() + "/ offset "
                    + data.offset() + "/ timestamp " + data.timestamp());
        };

        producer.send(records, callback).get();
    }

    Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
    }

    @Override
    public void close() {
        producer.close();
    }
}
