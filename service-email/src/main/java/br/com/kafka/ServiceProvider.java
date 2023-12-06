package br.com.kafka;

import br.com.kafka.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class ServiceProvider {
    public <T> void run(ServiceFactory<T> factory) throws ExecutionException, InterruptedException {
        var emailService = factory.create();
        try (var consumer = new KafkaConsumer(
                emailService.getConsumerGroup(),
                emailService.getConsumerGroup() + "_" + UUID.randomUUID().toString(),
                emailService.getTopic(),
                emailService::parse,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))) {
            consumer.run();
        }
    }
}
