package br.com.kafka;

import java.math.BigDecimal;

public class OrderDTO {

    private final String userId, orderId;
    private final BigDecimal amount;

    public OrderDTO(String userId, String orderId, BigDecimal amount) {
        this.userId = userId;
        this.orderId = orderId;
        this.amount = amount;
    }

    public String getUserId() {
        return userId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public String getEmail() {
        return "email";
    }
    @Override
    public String toString() {
        return "Order{" +
                "userId='" + userId + '\'' +
                ", orderId='" + orderId + '\'' +
                ", amount=" + amount +
                '}';
    }
}
