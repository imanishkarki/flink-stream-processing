package com.swift.flink_app.dto;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RemittanceTransaction {
    private String transactionId;
    private String senderId;
    private String receiverId;
    private double amount;
    private String currency;
    private double exchangeRate;
    private long timestamp;
}
