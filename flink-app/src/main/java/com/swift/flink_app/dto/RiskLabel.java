package com.swift.flink_app.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class RiskLabel {
    private String transactionId;
    private double amount;
    private double timeStamp;
    private String processingTime;
    private String  latency;
    private String riskType;
}