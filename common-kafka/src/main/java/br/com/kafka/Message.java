package br.com.kafka;

public class Message<T> {

    private final CurrelationId id;
    private final T payload;

    Message(CurrelationId id, T payload){
        this.id = id;
        this.payload = payload;
    }

    public CurrelationId getId() {
        return id;
    }

    public T getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "Message{" +
                "id=" + id +
                ", payload=" + payload +
                '}';
    }
}
