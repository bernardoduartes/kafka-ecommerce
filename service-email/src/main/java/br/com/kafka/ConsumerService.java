package br.com.kafka;

import br.com.kafka.model.EmailDTO;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerService<T> {
    void parse(ConsumerRecord<String, EmailDTO> record);
    String getTopic();
    String getConsumerGroup();
}
