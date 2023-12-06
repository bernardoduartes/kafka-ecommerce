package br.com.kafka;

public interface ServiceFactory<T> {
    ConsumerService<T> create();
}
