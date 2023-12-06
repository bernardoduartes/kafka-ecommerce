package br.com.kafka;

import br.com.kafka.consumer.KafkaConsumer;
import br.com.kafka.model.EmailDTO;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.spi.LocaleServiceProvider;

public class EmailConsumer  implements ConsumerService<String> {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        new ServiceProvider().run(EmailConsumer::new);
    }

    @Override
    public void parse(ConsumerRecord<String, EmailDTO> record) {
        System.out.println("------------------------------------------");
        System.out.println("Sending email:");
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
        System.out.println("Email sent");
        System.out.println("------------------------------------------");
    }

    @Override
    public String getConsumerGroup() {
        return EmailConsumer.class.getSimpleName();
    }
    @Override
    public String getTopic() {
        return "ECOMMERCE_SEND_EMAIL";
    }
}
