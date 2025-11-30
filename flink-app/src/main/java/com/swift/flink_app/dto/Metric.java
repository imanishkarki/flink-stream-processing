package com.swift.flink_app.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Metric {
    private long windowStart;
    private long windowEnd;
    private long count;
    private double avgAmount;
    private double avgExchangeRate;
    private double minAmount;
    private double maxAmount;
    private int successCount;
    private int failureCount;
}
