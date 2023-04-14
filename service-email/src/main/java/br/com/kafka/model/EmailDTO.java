package br.com.kafka.model;

public class EmailDTO {
    private final String subject, body;

    public EmailDTO(String subject, String body) {
        this.subject = subject;
        this.body = body;
    }
}
