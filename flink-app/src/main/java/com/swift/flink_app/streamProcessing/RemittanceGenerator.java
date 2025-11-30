package com.swift.flink_app.streamProcessing;

import com.github.javafaker.Faker;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Service
public class RemittanceGenerator {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final Faker faker = new Faker();
    private final Random random = new Random();

    @Value("${app.kafka.topic}")
    private String topicName;

    private static final String[] CURRENCIES = {
            "USD", "NPR", "INR", "CNY", "AUD", "EUR"
    };

    public RemittanceGenerator(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedRate = 2000)
    public void generateRemittanceStream() throws ExecutionException, InterruptedException {

        String transactionId = faker.internet().uuid();
        String senderId = "SENDER-" + random.nextInt(100000);
        String receiverId = "RECEIVER-" + random.nextInt(100000);
        //double amount = random.nextDouble(10000);
        double amount = random.nextDouble();
        String currency = CURRENCIES[random.nextInt(CURRENCIES.length)];
        double exchangeRate = generateExchangeRate(currency);
        long timestamp = System.currentTimeMillis();

        String json = String.format(
                "{ \"transactionId\": \"%s\", " +
                        "\"senderId\": \"%s\", " +
                        "\"receiverId\": \"%s\", " +
                        "\"amount\": %.2f, " +
                        "\"currency\": \"%s\", " +
                        "\"exchangeRate\": %.4f, " +
                        "\"timestamp\": %d }",
                transactionId,
                senderId,
                receiverId,
                amount,
                currency,
                exchangeRate,
                timestamp
        );

        CompletableFuture<?> future = kafkaTemplate.send(topicName, json);
        future.get();
        System.out.println("Produced: " + json);
    }

    private double generateExchangeRate(String currency) {
        return switch (currency) {
            case "EUR" -> 0.92;
            case "AUD" -> 1.54;
            case "INR" -> 133.0;
            case "CNY" -> 0;
            case "NPR" -> 133.5;
            default -> 1.0; // USD
        };
    }
}
