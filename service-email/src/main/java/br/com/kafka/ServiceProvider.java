package br.com.kafka;

import br.com.kafka.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class ServiceProvider<T> implements Callable<Void> {

    private final ServiceFactory<T> factory;

    public ServiceProvider(ServiceFactory<T> factory) {
        this.factory = factory;
    }
    public Void call() throws ExecutionException, InterruptedException {
        var myService = factory.create();
        var clientIdConfig = myService.getConsumerGroup() + "_" + UUID.randomUUID().toString();
        try (var consumer = new KafkaConsumer(
                myService.getConsumerGroup(),
                clientIdConfig,
                myService.getTopic(),
                myService::parse,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))) {
            consumer.run();
        }
        return null;
    }
}
