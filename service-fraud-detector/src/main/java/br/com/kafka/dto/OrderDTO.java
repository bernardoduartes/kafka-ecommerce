package br.com.kafka.dto;

import java.math.BigDecimal;

public class OrderDTO {

    private final String orderId;
    private final BigDecimal amount;
    private final String email;

    public OrderDTO(String orderId, BigDecimal amount, String email) {

        this.orderId = orderId;
        this.amount = amount;
        this.email = email;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public String getEmail() {
        return email;
    }

    @Override
    public String toString() {
        return "OrderDTO{" +
                "orderId='" + orderId + '\'' +
                ", amount=" + amount +
                ", email='" + email + '\'' +
                '}';
    }
}
