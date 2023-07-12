package br.com.kafka;

import java.util.UUID;

public class CurrelationId {

    private final String id;

    public CurrelationId() {
        this.id = UUID.randomUUID().toString();
    }

    @Override
    public String toString() {
        return "CurrelationId{" +
                "id='" + id + '\'' +
                '}';
    }
}
