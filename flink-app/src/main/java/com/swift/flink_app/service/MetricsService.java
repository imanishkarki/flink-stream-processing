package com.swift.flink_app.service;

import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Spring Boot Service to fetch metrics from Prometheus and push them to the frontend
 * via WebSockets (STOMP).
 */
@Service
public class MetricsService {

    // Inject SimpMessagingTemplate for sending messages over WebSocket (STOMP)
    private final SimpMessagingTemplate template;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    // Configuration for the metrics to query (PromQL queries)
    private static final Map<String, String> METRICS_QUERIES = Map.of(
            // Queries contain spaces and parentheses, requiring URL encoding.
            "input_rate", "sum(flink_taskmanager_job_task_operator_numRecordsInPerSecond) by (job_name)",
            "output_rate", "sum(flink_taskmanager_job_task_operator_numRecordsOutPerSecond) by (job_name)",
            "checkpoint_size", "flink_jobmanager_job_lastCheckpointSize"
    );

    // FIX: Changed 'prometheus' to 'localhost' to resolve java.nio.channels.UnresolvedAddressException,
    // assuming Prometheus is exposed on the host machine at port 9090.
    private static final String PROMETHEUS_URL = "http://localhost:9070/api/v1/query?query=";

    public MetricsService(SimpMessagingTemplate template) {
        this.template = template;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Executes every 5 seconds to query Prometheus and broadcast metrics via WebSocket.
     */
    @Scheduled(fixedRate = 5000)
    public void fetchAndBroadcastMetrics() {
        for (Map.Entry<String, String> entry : METRICS_QUERIES.entrySet()) {
            String metricId = entry.getKey();
            String query = entry.getValue();

            try {
                // 1. Fetch data from Prometheus
                String resultJson = queryPrometheus(query);

                // 2. Parse and format the data
                JsonNode root = objectMapper.readTree(resultJson);
                JsonNode result = root.at("/data/result/0");

                if (result.isMissingNode() || result.isEmpty()) {
                    System.out.println("No data returned for metric: " + metricId + ". Prometheus likely not receiving Flink data.");
                    continue; // Skip if no data
                }

                String jobName = result.at("/metric/job_name").asText("N/A");
                double rawValue = result.at("/value/1").asDouble();
                String formattedValue;

                // Handle checkpoint size conversion (bytes to MB)
                if (metricId.equals("checkpoint_size")) {
                    formattedValue = String.format("%.2f", rawValue / (1024 * 1024));
                } else {
                    formattedValue = String.format("%.2f", rawValue);
                }

                // 3. Prepare payload (matching what the frontend expects)
                Map<String, String> payload = Map.of(
                        "id", metricId,
                        "value", formattedValue,
                        "jobName", jobName
                );

                // 4. Push to WebSocket topic (simulating real-time push)
                // The frontend client will subscribe to /topic/metrics
                template.convertAndSend("/topic/metrics", payload);

            } catch (Exception e) {
                // IMPORTANT: Print stack trace to debug connection issues
                String errorMsg = e.getMessage() != null ? e.getMessage() : e.getClass().getName() + " (Check if Prometheus is running on http://localhost:9090)";
                System.err.println("Error fetching metric " + metricId + ": " + errorMsg);
                e.printStackTrace();
            }
        }
    }

    /**
     * Executes the PromQL query against the Prometheus API.
     *
     * @param query The raw PromQL query string.
     * @return The raw JSON response body from Prometheus.
     * @throws Exception if the request fails or the status code is not 200.
     */
    private String queryPrometheus(String query) throws Exception {
        // --- URL encode the query string to handle special characters (spaces, parentheses) ---
        String encodedQuery = URLEncoder.encode(query, StandardCharsets.UTF_8.toString());

        // Construct the full URI using the encoded query
        URI uri = new URI(PROMETHEUS_URL + encodedQuery);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(uri)
                .timeout(Duration.ofSeconds(10))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new RuntimeException("Prometheus query failed with status code: " + response.statusCode() + " for query: " + query + ". Response body: " + response.body());
        }

        return response.body();
    }
}