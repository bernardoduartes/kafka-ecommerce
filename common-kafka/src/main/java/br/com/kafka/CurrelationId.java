package br.com.kafka;

import java.util.UUID;

public class CurrelationId {

    private final String id;

    public CurrelationId(String title) {
        this.id = title + "("+UUID.randomUUID().toString()+")";
    }

    @Override
    public String toString() {
        return "CurrelationId{" +
                "id='" + id + '\'' +
                '}';
    }

    public CurrelationId continueWith(String title) {
        return new CurrelationId(id + "-" + title);
    }
}
