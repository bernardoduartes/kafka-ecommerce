package br.com.kafka;

import br.com.kafka.consumer.ConsumerService;
import br.com.kafka.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.ExecutionException;

public class EmailConsumer  implements ConsumerService<String> {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        new ServiceRunner(EmailConsumer::new).start(3);
    }

    @Override
    public void parse(ConsumerRecord<String, Message<String>> record) {
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
