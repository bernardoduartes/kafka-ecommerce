package br.com.kafka.consumer;

public interface ServiceFactory<T> {
    ConsumerService<T> create();
}
