package br.com.kafka.dto;

import java.math.BigDecimal;

public class OrderDTO {

    private final String userId, orderId;
    private final BigDecimal amount;

    public OrderDTO(String userId, String orderId, BigDecimal amount) {
        this.userId = userId;
        this.orderId = orderId;
        this.amount = amount;
    }
}
